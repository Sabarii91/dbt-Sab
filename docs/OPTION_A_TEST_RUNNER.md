# Option A: Test runner (CSV as only config)

Row-level checks are driven by a **seed CSV**. By default the macro uses **`test_config_full_load`** (full-load `comprehensive_compare` only). Use **`test_config`** when you also want delta-slice compares and `delta_compare` — that requires **all** validation models under `models/validation/` (full + delta source + delta slices) to exist after `dbt run`.

**Flow:** The macro reads the seed table and runs `comprehensive_compare` / `delta_compare` per row. Results are appended to **QA failure tables only** (no summary audit table managed by dbt).

## What was implemented

| Piece | Location | Purpose |
|-------|----------|--------|
| **Config** | `seeds/test_config_full_load.csv` (default) or `seeds/test_config.csv` | Same columns; full-load seed only references `*_full_load_source` models. |
| **Validation full views** | `models/validation/full/*.sql` | Full-load source SQL for `comprehensive_compare` (`test_query_model`). |
| **Validation delta source views** | `models/validation/delta/source/*.sql` | Delta source SQL for `comprehensive_compare` (`test_query_model`). |
| **Validation delta slices** | `models/validation/delta/slices/*.sql` | SILVER views for delta `comprehensive_compare` / `delta_compare` targets. |
| **Runner macro** | `macros/run_validation_tests.sql` | Creates (if needed) and inserts into `comprehensive_compare_<model>_failures` and `delta_compare_<model>_failures` in the QA schema. QA database/schema are taken from `ref(qa_anchor_model)` (default `stg_customers_full_load_source`; override with `--args`). |
| **Staging YAML** | `models/staging/raw_silver/_stg__models.yml` | Model metadata; generic `dbt test` definitions where used. |
| **BEFORE_DELTA clones** | `stg_customers` / `stg_material` hooks | `DATABASE.{before_delta_schema}.{model}_BEFORE_DELTA` (default schema `QA` via `dbt_project.yml` `vars`). `get_before_delta_table_ref` must stay aligned. |

## How to run

1. **Build models and seeds**
   ```bash
   dbt seed
   dbt run
   ```

2. **Run the validation macro** (defaults to full-load config)
   ```bash
   dbt run-operation run_validation_tests
   ```

   **Full suite** (delta slice + `delta_compare`; needs `stg_*_delta_source` views built):
   ```bash
   dbt run-operation run_validation_tests --args "{\"config_seed\": \"test_config\"}"
   ```
   On cmd.exe you can use: `--args "{config_seed: test_config}"` if your shell accepts it.

   **QA schema anchor** (if failure tables should resolve DB/schema from another `models/validation/full/` model):
   ```bash
   dbt run-operation run_validation_tests --args "{\"qa_anchor_model\": \"stg_material_full_load_source\"}"
   ```

   **Filter one model from a shared config seed**:
   ```bash
   dbt run-operation run_validation_tests --args "{\"config_seed\": \"test_config\", \"model_name_filter\": \"stg_customers_delta_slice\"}"
   ```

   **Filter by model pattern (`LIKE`)**:
   ```bash
   dbt run-operation run_validation_tests --args "{\"config_seed\": \"test_config\", \"model_name_like\": \"stg_customers%\"}"
   ```

   **Filter by model + test type**:
   ```bash
   dbt run-operation run_validation_tests --args "{\"config_seed\": \"test_config\", \"model_name_filter\": \"stg_customers_delta_slice\", \"test_type_filter\": \"delta_compare\"}"
   ```

3. **Typical pipeline**
   ```bash
   dbt seed
   dbt run
   dbt run-operation run_validation_tests
   ```

## Inspect results

- **Row-level failures:** `QA.comprehensive_compare_<model_name>_failures`, `QA.delta_compare_<model_name>_failures`.
- **dbt tests:** use `dbt test` with `store_failures: true` (failure tables in QA, dbt-named).

## If you see “delta_source does not exist”

`seeds/test_config.csv` includes rows that use `stg_*_delta_source`. Those are **views** built by `dbt run`. Either run a **full** `dbt run` (including `models/validation/`) before using `test_config`, or stay on the default **`test_config_full_load`** so only `*_full_load_source` models are referenced.

## Editing config

Add or change rows in `test_config_full_load.csv` and/or `test_config.csv`, then `dbt seed` and re-run the operation (pass `config_seed` when not using the default).

## Seed columns (both CSVs)

| Column | Used for | Example |
|--------|----------|--------|
| model_name | All | `stg_customers_delta_slice` |
| test_type | All | `comprehensive_compare`, `delta_compare` only |
| config_set | Reporting (optional) | `FULL_LOAD`, `DELTA`, `DELTA_SLICE` |
| key_column | All | `CUSTOMER_ID` |
| test_query_model | comprehensive_compare | `stg_customers_delta_source` |
| compare_columns / compare_columns_ref | comprehensive_compare, delta_compare | Column list or seed ref |
| base_model | delta_compare | `stg_customers` |
| clone_suffix | delta_compare | `_BEFORE_DELTA` |

Empty cells are allowed where a column does not apply.
