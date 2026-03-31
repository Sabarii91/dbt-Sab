# DBT Project Strategy: Automated Testing & Building Tables

A practical strategy for structuring your DBT project, building tables reliably, and running automated tests.

---

## 1. Project Layout (Medallion / Layered Architecture)

Use a clear **layer-based** folder structure so raw → staging → intermediate → marts flow is obvious and testable.

```
models/
├── staging/           # Raw → cleaned, typed, renamed (one model per source table)
│   └── customer/
│       ├── stg_customers.sql
│       ├── _stg__models.yml
│       └── _raw__sources.yml
├── intermediate/      # Business logic, joins (optional)
│   └── int_*.sql
├── marts/             # Final reporting tables
│   └── core/
│       └── dim_customers.sql
├── _sources.yml       # Optional: project-level sources
└── _models.yml        # Optional: project-level config
```

**Why this helps**

- **Build order**: Staging → intermediate → marts. Use `ref()` only downward so `dbt run` respects dependencies.
- **Testing**: Test staging for schema/quality; test marts for business rules and row counts.
- **Automation**: Run `dbt run` then `dbt test` in CI; same commands locally.

---

## 2. Building Tables: Strategy

### 2.1 Materialization by layer

| Layer        | Default materialization | Reason |
|-------------|--------------------------|--------|
| **Staging** | `view` or `table`        | Views = fast dev; tables = faster downstream if heavy. Your project uses `table` in SILVER—good for production. |
| **Intermediate** | `view`           | Ephemeral logic; often no need to persist. |
| **Marts**   | `table`                  | Final assets; tables give predictable performance. |

Configure in `dbt_project.yml` (you already do this for staging):

```yaml
models:
  dbt_basic:
    staging:
      +materialized: table
      +schema: SILVER
    marts:
      +materialized: table
      +schema: GOLD  # or your convention
```

### 2.2 Incremental where possible

For large, append-only or slowly changing source tables, use **incremental** models so you only process new/changed rows:

```sql
{{
  config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge'  # or 'append' / 'delete+insert' by platform
  )
}}
select ...
from {{ source('raw', 'CUST_SALES') }}
{% if is_incremental() %}
  where CREATED_DT > (select max(CREATED_TIMESTAMP) from {{ this }})
{% endif %}
```

### 2.3 Build order and selection

- **Full build**: `dbt run` — builds all models in dependency order.
- **Only changed**: `dbt run --select state:modified+` (with `--state` from prior run) for CI.
- **Single model + deps**: `dbt run --select stg_customers+` to build that model and all downstream.

Use tags for groups:

```yaml
# In model YAML or in config block
models:
  - name: stg_customers
    config:
      tags: ['staging', 'customer']
```

Then: `dbt run --select tag:staging`.

---

## 3. Testing Strategy

### 3.1 Test pyramid

1. **Source freshness** — `dbt source freshness` so raw data is loaded on time.
2. **Staging** — Uniqueness, not null, data types, and **accepted values** for key columns.
3. **Marts** — Row-count or reconciliation tests, critical business rules.

### 3.2 What to use where

| Test type | Where | Purpose |
|-----------|--------|---------|
| **Generic (schema) tests** | Staging + marts | `unique`, `not_null`, `accepted_values`, `relationships` (FK). |
| **Singular tests** (`tests/*.sql`) | Staging / marts | Row-count reconciliation, tolerance, custom SQL. |
| **Source freshness** | Sources | Alert when raw data is stale. |

You already have:

- `_stg__models.yml`: `unique`, `not_null` on `stg_customers` (and duplicate for `test_stg_customers`).
- Singular tests: `assert_row_count.sql`, `assert_row_tolerance.sql`.

**Recommendation**: Prefer one source of truth (e.g. only `stg_customers`); remove or repurpose `test_stg_customers` so tests and docs point at the real model.

### 3.3 Add accepted_values for critical columns

In `_stg__models.yml` (or equivalent), add accepted_values for any domain-controlled columns, e.g.:

```yaml
- name: SALES_REP_TYPE
  tests:
    - accepted_values:
        values: ['SA', 'AM', 'IS']
        quote: false
```

(Adjust to your actual allowed values and quoting.)

### 3.4 Source freshness

In your source YAML (e.g. `_raw__sources.yml`):

```yaml
tables:
  - name: CUST_SALES
    description: "Raw customer data"
    freshness:
      warn_after: { count: 24, period: hour }
      error_after: { count: 48, period: hour }
    loaded_at_field: CREATED_DT   # or your timestamp column
```

Then run: `dbt source freshness`.

### 3.5 Singular tests: row count and tolerance

- **Strict row count**: “Silver count must equal raw count.” Use a singular test that returns rows only when counts differ (see corrected example in `tests/`).
- **Tolerance**: “Difference between raw and silver is at most N.” Your `assert_row_tolerance.sql` follows this; keep it and run it in CI.

### 3.6 QA failure tables (`store_failures: true`)

When a test has `store_failures: true`, dbt materializes the test query result into a table in the **QA** schema (e.g. `ZIMMERPOC.QA.<test_name_hash>`). You can query that table to see which rows failed.

**Why a specific QA table might not exist**

1. **The test errored before writing** — If the test hits a database error (e.g. type mismatch when inserting mixed types into one column), the materialization can fail and the table is never created or is rolled back. Fix: ensure the test output uses a single type (e.g. cast `model_value` / `expected_value` to `VARCHAR`) so the failures table can be created.
2. **The table name hash changed** — The table name includes a hash of the test config (e.g. `compare_with_query_stg_customers_..._1fb4c8b...`). Changing `test_query`, `key_column`, or `compare_columns` changes the hash, so the table name changes and the old one may have been dropped or never created for the new config.
3. **Database/schema/role** — The table is created in the target database and schema from your dbt profile (e.g. `ZIMMERPOC.QA`). If you run in a different Snowflake role or database context, use the fully qualified name or check `SHOW TABLES`.

**How to see which failure tables exist**

In Snowflake (use the same database/schema as your dbt target):

```sql
-- List all tables in QA schema
SHOW TABLES IN ZIMMERPOC.QA;

-- Or filter by name
SELECT TABLE_NAME
FROM ZIMMERPOC.QA.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE 'COMPARE_WITH%';
```

Then query the table name you see, e.g. `SELECT * FROM ZIMMERPOC.QA.COMPARE_WITH_QUERY_STG_CUSTOME_<hash>`.

---

## 4. Automated Testing and Building (CI)

### 4.1 Local workflow

```bash
# 1. Install deps (if you use packages)
dbt deps

# 2. Build all models
dbt run

# 3. Run all tests
dbt test

# 4. Optional: source freshness
dbt source freshness
```

### 4.2 CI pipeline (e.g. GitHub Actions)

- On every push/PR: run `dbt run` then `dbt test` (and optionally `dbt source freshness`).
- Use a dedicated DB user and connection (env vars or CI secrets); typically a dev or CI schema.
- Optional: `dbt run --select state:modified+` and `dbt test --select state:modified+` with `--state` from a previous run artifact to only build/test changed nodes.

A sample workflow is in `.github/workflows/dbt_ci.yml`.

### 4.3 What “automated” gives you

- **Build**: Tables/views are created or updated on every run.
- **Test**: Generic + singular tests run after each build; failures fail the pipeline.
- **Freshness**: Stale source data can fail or warn in the same pipeline.

---

## 5. Quick Checklist

- [ ] One staging model per source table; consistent naming (`stg_*`).
- [ ] Sources defined in YAML with freshness where applicable.
- [ ] Schema tests on PKs and critical columns (unique, not_null, accepted_values).
- [ ] Singular tests for row-count reconciliation and tolerance where needed.
- [ ] Marts as tables; staging/intermediate chosen by need (view vs table, incremental).
- [ ] CI runs `dbt run` and `dbt test` (and optionally `dbt source freshness`) on every change.
- [ ] Remove or repurpose duplicate models (e.g. `test_stg_customers`) so all tests target the real model.

---

## 6. References

- [dbt docs: Project structure](https://docs.getdbt.com/guides/best-practices/how-we-structure-our-dbt-projects)
- [dbt docs: Testing](https://docs.getdbt.com/docs/build/tests)
- [dbt docs: Incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt docs: Source freshness](https://docs.getdbt.com/docs/deploy/source-freshness)
