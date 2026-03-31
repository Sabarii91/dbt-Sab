Welcome to your new dbt project!

### Quick start

```bash
dbt deps    # install packages (if any)
dbt run     # build all models
dbt test    # run schema + singular tests
```

### Run report (user-friendly summary)

After each run, you can generate a readable report from the dbt log:

- **Run dbt and then the report:**  
  `dbt build` (or `dbt run` / `dbt test`), then:  
  `python scripts/dbt_run_report.py`
- **One command (runs dbt then report):**  
  `.\scripts\run_dbt_with_report.ps1 build`  
  or `scripts\run_dbt_with_report.bat run --select staging`

The script reads `target/run_results.json` and prints a console summary and writes `target/dbt_run_report.html` for an easy-to-scan overview of what ran, pass/fail, timing, and row counts.

### HTML report and logs per run

To run dbt and **save an HTML report plus log files** in a timestamped directory for future reference:

- Run: **`scripts\run_dbt_build_and_report.bat`**  
  This runs `dbt build`, then creates **`reports/YYYYMMDD_HHMMSS/`** with:
  - **`dbt_run_report.html`** — light, compact report with status and duration charts (open in any browser)
  - **`run_results.json`** — dbt run results
  - **`logs/`** — copy of dbt log files from that run

No Streamlit required; the HTML report is self-contained.

### Strategy and automation

- **Strategy and testing guide**: [docs/DBT_STRATEGY_AND_TESTING.md](docs/DBT_STRATEGY_AND_TESTING.md) — project layout, building tables, testing pyramid, and CI.
- **CI**: `.github/workflows/dbt_ci.yml` runs `dbt run` and `dbt test` on push/PR. Configure warehouse credentials in repo Secrets to run against your dev/CI database.

### Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
