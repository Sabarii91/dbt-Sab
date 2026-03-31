#!/usr/bin/env python3
"""
DBT Run Report – Streamlit UI

View the latest or any archived dbt run with a business-friendly report.
Saved runs are stored under reports/<timestamp>/ for future reference.

Run: streamlit run scripts/dbt_run_report_app.py
  (from project root, or use run_dbt_build_and_report.bat)
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

# Allow importing dbt_run_report from same directory
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import dbt_run_report as report_lib
import streamlit as st

# ---------------------------------------------------------------------------
# Project and saved runs
# ---------------------------------------------------------------------------

def get_project_dir() -> Path | None:
    return report_lib.find_project_dir(_SCRIPT_DIR)


def get_project_name(project_dir: Path) -> str:
    name = "dbt"
    path = project_dir / "dbt_project.yml"
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith("name:"):
                        name = line.split("name:")[-1].strip().strip("'\"").strip()
                        break
        except OSError:
            pass
    return name


def list_saved_runs(project_dir: Path) -> list[tuple[str, Path]]:
    """Return list of (display_label, run_results_path) sorted newest first."""
    reports_dir = project_dir / "reports"
    if not reports_dir.is_dir():
        return []

    runs = []
    for d in reports_dir.iterdir():
        if not d.is_dir():
            continue
        run_results = d / "run_results.json"
        if not run_results.is_file():
            continue
        # Format folder name 20250305_143022 -> 2025-03-05 14:30:22
        raw = d.name
        if len(raw) == 15 and raw[8] == "_":
            try:
                display = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]} {raw[9:11]}:{raw[11:13]}:{raw[13:15]}"
            except Exception:
                display = raw
        else:
            display = raw
        runs.append((display, run_results))

    runs.sort(key=lambda x: x[1].stat().st_mtime, reverse=True)
    return runs


def get_latest_target_path(project_dir: Path) -> Path:
    return project_dir / "target" / "run_results.json"


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

def run_app() -> None:
    project_dir = get_project_dir()
    if not project_dir:
        st.error("Could not find dbt project (dbt_project.yml). Run from project root or set project dir.")
        return

    project_name = get_project_name(project_dir)
    st.set_page_config(page_title=f"DBT Run Report – {project_name}", layout="wide")
    st.title(f"DBT Run Report")
    st.caption(f"Project: {project_name}")

    # Run selector
    saved_runs = list_saved_runs(project_dir)
    latest_path = get_latest_target_path(project_dir)
    has_latest = latest_path.is_file()

    options: list[tuple[str, Path | None]] = []
    if has_latest:
        options.append(("Latest (from target/)", latest_path))
    for label, path in saved_runs:
        options.append((label, path))
    if not options:
        st.warning("No run results found. Run `dbt build` (or use the batch file) to generate results and archive them.")
        return

    labels = [o[0] for o in options]
    selected_label = st.selectbox("Select run", labels, index=0)
    selected_path = next(p for l, p in options if l == selected_label)

    report = report_lib.build_report_data_from_file(selected_path)
    if not report:
        st.error("Failed to load run results for this run.")
        return

    # Executive summary
    c = report["counts"]
    total = report["total_nodes"]
    passed = c["success"] + c["pass"]
    failed = c["fail"] + c["error"]
    skipped = c["skip"]
    overall = "Success" if report["overall_success"] else "Failed"
    elapsed = report_lib.format_duration(report.get("elapsed_time"))

    st.subheader("Run summary")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total", total)
    col2.metric("Passed", passed)
    col3.metric("Failed", failed)
    col4.metric("Skipped", skipped)
    col5.metric("Duration", elapsed)

    status_color = "green" if report["overall_success"] else "red"
    st.markdown(f"**Overall: :{status_color}[{overall}]**")
    st.caption(f"Command: {report.get('command', '—')}  |  Finished: {report.get('generated_at', '—')}")

    # Details table
    st.subheader("Details")
    rows = report["rows"]
    if not rows:
        st.info("No nodes in this run.")
    else:
        # Build display table: Type, Node, Status, Time, Rows, Failures, Message
        import pandas as pd
        table_data = []
        for r in rows:
            table_data.append({
                "Type": r["type"],
                "Node": r["name"],
                "Status": r["status"].upper(),
                "Time": report_lib.format_duration(r.get("execution_time")),
                "Rows": r["rows_affected"] if r.get("rows_affected") is not None else "—",
                "Failures": r["failures"] if r.get("failures") is not None else "—",
                "Message": (r.get("message") or "—")[:200],
            })
        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Save snapshot (HTML) for future reference
    st.divider()
    snapshot_dir: Path | None = None
    if selected_label.startswith("Latest"):
        snapshot_dir = project_dir / "target"
    else:
        # Selected run is from reports/YYYYMMDD_HHMMSS/
        snapshot_dir = selected_path.parent if selected_path else None

    if snapshot_dir and snapshot_dir.is_dir():
        snapshot_path = snapshot_dir / "dbt_run_report.html"
        if st.button("Save snapshot (HTML) for future reference"):
            html = report_lib.build_html_report(report, project_name=project_name)
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(html, encoding="utf-8")
            st.success(f"Saved to: {snapshot_path}")
            st.caption("You can open this file in any browser without running Streamlit.")


if __name__ == "__main__":
    run_app()
