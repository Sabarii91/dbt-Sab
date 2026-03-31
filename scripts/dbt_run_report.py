#!/usr/bin/env python3
"""
DBT Run Report Generator

Reads target/run_results.json after a dbt run and produces a user-friendly
console summary and an HTML report for easy understanding of what happened.

Usage:
  python scripts/dbt_run_report.py [--project-dir PATH] [--output PATH]
  # Or run after dbt:  dbt build && python scripts/dbt_run_report.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_project_dir(start: Path | None = None) -> Path | None:
    """Locate dbt project root (directory containing dbt_project.yml)."""
    start = start or Path.cwd()
    for d in [start, *start.parents]:
        if (d / "dbt_project.yml").exists():
            return d
    return None


def load_json(path: Path) -> dict | None:
    """Load JSON file; return None if missing or invalid."""
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def friendly_node_name(unique_id: str) -> str:
    """Turn unique_id into a short, readable name (e.g. model.dbt_basic.stg_customers -> stg_customers)."""
    if not unique_id:
        return unique_id
    parts = unique_id.split(".")
    if len(parts) >= 3:
        name = parts[-1]
        # If last part looks like a short hash (e.g. 51df897520), use the main test/model name instead
        if len(parts) >= 4 and parts[-1].replace("_", "").isalnum() and 6 <= len(parts[-1]) <= 16:
            name = parts[-2]  # e.g. compare_with_query_stg_customers_...
        elif len(name) >= 32 and name.replace("_", "").isalnum():
            name = parts[-2] if len(parts) > 3 else name
        if len(name) > 48:
            name = name[:45] + "..."
        return name
    return unique_id


def node_type(unique_id: str) -> str:
    """Return resource type: model, test, seed, snapshot, etc."""
    if not unique_id:
        return "unknown"
    parts = unique_id.split(".")
    return parts[0] if parts else "unknown"


def format_duration(seconds: float) -> str:
    if seconds is None or seconds < 0:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    m = int(seconds // 60)
    s = seconds % 60
    return f"{m}m {s:.1f}s"


def safe_get(obj: dict, *keys, default=None):
    for k in keys:
        if isinstance(obj, dict) and k in obj:
            obj = obj[k]
        else:
            return default
    return obj


# ---------------------------------------------------------------------------
# Report data from run_results.json
# ---------------------------------------------------------------------------

def _process_results_data(data: dict) -> dict:
    """Turn raw run_results.json dict into structured report data."""
    metadata = data.get("metadata") or {}
    results = data.get("results") or []
    elapsed = data.get("elapsed_time")
    args = data.get("args") or {}

    counts = {"success": 0, "pass": 0, "fail": 0, "error": 0, "skip": 0}
    for r in results:
        s = (r.get("status") or "").lower()
        if s in counts:
            counts[s] += 1

    rows = []
    for r in results:
        uid = r.get("unique_id") or ""
        status = (r.get("status") or "unknown").lower()
        exec_time = r.get("execution_time")
        adapter = r.get("adapter_response") or {}
        rows_affected = adapter.get("rows_affected")
        if rows_affected is None and "rows_affected" in r:
            rows_affected = r["rows_affected"]
        failures = r.get("failures")
        message = r.get("message") or ""
        relation = r.get("relation_name") or ""

        rows.append({
            "unique_id": uid,
            "name": friendly_node_name(uid),
            "type": node_type(uid),
            "status": status,
            "execution_time": exec_time,
            "rows_affected": rows_affected,
            "failures": failures,
            "message": message,
            "relation_name": relation,
        })

    command = args.get("invocation_command") or args.get("which") or "dbt"
    if isinstance(command, list):
        command = " ".join(str(c) for c in command)

    return {
        "generated_at": metadata.get("generated_at"),
        "dbt_version": metadata.get("dbt_version"),
        "invocation_id": metadata.get("invocation_id"),
        "command": command,
        "elapsed_time": elapsed,
        "counts": counts,
        "total_nodes": len(results),
        "rows": rows,
        "overall_success": (counts["fail"] + counts["error"]) == 0,
    }


def build_report_data(project_dir: Path) -> dict | None:
    """Parse run_results.json from project target; return structured data for report."""
    run_results_path = project_dir / "target" / "run_results.json"
    return build_report_data_from_file(run_results_path)


def build_report_data_from_file(run_results_path: Path) -> dict | None:
    """Parse run_results.json from given path; return structured data for report."""
    data = load_json(run_results_path)
    if not data:
        return None
    return _process_results_data(data)


# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------

def print_console_report(report: dict) -> None:
    """Print a user-friendly summary to stdout."""
    if not report:
        print("No run results found. Run dbt first (e.g. dbt run, dbt build, dbt test).")
        return

    c = report["counts"]
    total = report["total_nodes"]
    ok = c["success"] + c["pass"]
    bad = c["fail"] + c["error"]
    skipped = c["skip"]

    print()
    print("=" * 60)
    print("  DBT RUN REPORT")
    print("=" * 60)
    print(f"  Command:     {report['command']}")
    print(f"  Finished:    {report.get('generated_at', '—')}")
    print(f"  Total time:  {format_duration(report.get('elapsed_time'))}")
    print("-" * 60)
    print(f"  Total: {total}  |  OK: {ok}  |  Failed: {bad}  |  Skipped: {skipped}")
    print("=" * 60)

    if report["overall_success"]:
        print("  Overall: SUCCESS")
    else:
        print("  Overall: FAILED (see failed nodes below)")

    print()
    print("  Nodes:")
    print("-" * 60)
    for row in report["rows"]:
        status = row["status"].upper()
        if row["status"] in ("success", "pass"):
            status_icon = "[OK]"
        elif row["status"] in ("fail", "error"):
            status_icon = "[FAIL]"
        else:
            status_icon = "[SKIP]"
        name = row["name"][:40] + "..." if len(row["name"]) > 43 else row["name"]
        time_str = format_duration(row.get("execution_time"))
        extra = ""
        if row.get("rows_affected") is not None and row["type"] == "model":
            extra = f"  rows={row['rows_affected']}"
        elif row.get("failures") is not None and row["failures"]:
            extra = f"  failures={row['failures']}"
        if row.get("message"):
            extra = (extra + "  " + row["message"])[:50]
        print(f"  {status_icon:8}  {name:43}  {time_str:>8}  {extra}")
    print()


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def escape_html(s: str) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def build_html_report(report: dict, project_name: str = "dbt") -> str:
    """Build a single HTML document with:
    - clear summary
    - failures table at the top
    - charts
    - full detail table
    """
    if not report:
        return "<!DOCTYPE html><html><body><p>No run results found.</p></body></html>"

    c = report["counts"]
    total = report["total_nodes"]
    ok = c["success"] + c["pass"]
    bad = c["fail"] + c["error"]
    skipped = c["skip"]
    overall = "SUCCESS" if report["overall_success"] else "FAILED"
    overall_class = "overall-ok" if report["overall_success"] else "overall-fail"

    # Split rows into failures and others
    failures = []
    for r in report["rows"]:
        status = (r["status"] or "").lower()
        if status in ("fail", "error"):
            failures.append(r)

    # Top nodes by duration for bar chart (max 10)
    rows_with_time = [(r, r.get("execution_time") or 0) for r in report["rows"]]
    rows_with_time.sort(key=lambda x: -x[1])
    top_duration = rows_with_time[:10]
    duration_labels = [
        escape_html(r["name"][:30] + ("..." if len(r["name"]) > 30 else ""))
        for r, _ in top_duration
    ]
    duration_values = [round(t, 1) for _, t in top_duration]

    def row_html(row: dict) -> str:
        status = row["status"].lower()
        if status in ("success", "pass"):
            row_class = "row-ok"
        elif status in ("fail", "error"):
            row_class = "row-fail"
        else:
            row_class = "row-skip"
        time_str = format_duration(row.get("execution_time"))
        ra = row.get("rows_affected")
        rows_affected_str = str(ra) if ra is not None and ra >= 0 else "—"
        failures_str = str(row["failures"]) if row.get("failures") is not None else "—"
        msg = escape_html((row.get("message") or "")[:200])
        rel = escape_html((row.get("relation_name") or "")[:80])
        name = escape_html(row["name"])
        return (
            f'<tr class="{row_class}">'
            f"<td>{escape_html(row['type'])}</td>"
            f'<td><span class="node-name">{name}</span></td>'
            f'<td><span class="status status-{status}">{row["status"].upper()}</span></td>'
            f"<td>{time_str}</td>"
            f"<td>{rows_affected_str}</td>"
            f"<td>{failures_str}</td>"
            f'<td title="{rel}">{msg or "—"}</td>'
            "</tr>"
        )

    failures_body = "\n".join(row_html(r) for r in failures) or (
        '<tr><td colspan="7">No failed nodes</td></tr>'
    )
    all_body = "\n".join(row_html(r) for r in report["rows"])

    generated = report.get("generated_at") or "—"
    command = escape_html(report.get("command") or "—")
    elapsed = format_duration(report.get("elapsed_time"))

    chart_data = json.dumps(
        {
            "passed": ok,
            "failed": bad,
            "skipped": skipped,
            "durationLabels": duration_labels,
            "durationValues": duration_values,
        }
    ).replace("<", "\\u003c").replace(">", "\\u003e")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DBT Run Report – {escape_html(project_name)}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{ --bg: #f5f7fa; --card: #fff; --text: #1f2937; --muted: #6b7280;
             --ok: #059669; --fail: #dc2626; --skip: #6b7280; --accent: #2563eb;
             --border: #e5e7eb; }}
    * {{ box-sizing: border-box; }}
    body {{ font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
           background: var(--bg); color: var(--text); margin: 0;
           padding: 0.75rem 1rem; line-height: 1.4; font-size: 0.875rem; }}
    .container {{ max-width: 1100px; margin: 0 auto; }}
    h1 {{ font-size: 1.25rem; margin: 0 0 0.6rem; font-weight: 600; }}
    .summary {{ background: var(--card); border: 1px solid var(--border);
               border-radius: 6px; padding: 0.6rem 0.9rem; margin-bottom: 0.75rem; }}
    .summary-grid {{ display: grid;
                    grid-template-columns: repeat(auto-fill, minmax(120px, 1fr));
                    gap: 0.5rem; margin-top: 0.4rem; }}
    .badge {{ display: inline-block; padding: 0.1rem 0.4rem; border-radius: 999px;
              font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
              letter-spacing: 0.04em; }}
    .badge-ok {{ background:#d1fae5; color:#065f46; }}
    .badge-fail {{ background:#fee2e2; color:#991b1b; }}
    .badge-skip {{ background:#e5e7eb; color:#374151; }}
    .overall {{ font-size: 0.95rem; margin-top: 0.5rem; padding-top: 0.5rem;
               border-top: 1px solid var(--border); font-weight: 600; }}
    .overall-ok {{ color: var(--ok); }}
    .overall-fail {{ color: var(--fail); }}
    .meta {{ font-size: 0.75rem; color: var(--muted); margin-top: 0.4rem; }}
    .section {{ margin-bottom: 0.9rem; }}
    .section h2 {{ font-size: 0.95rem; margin: 0 0 0.4rem; font-weight: 600; }}
    .charts {{ display: flex; flex-wrap: wrap; gap: 0.75rem; }}
    .chart-box {{ background: var(--card); border: 1px solid var(--border);
                 border-radius: 6px; padding: 0.5rem; flex: 1 1 240px; }}
    .chart-box h3 {{ margin: 0 0 0.25rem; font-size: 0.8rem; font-weight: 600;
                    color: var(--muted); text-transform: uppercase;
                    letter-spacing: 0.03em; }}
    .chart-wrap {{ position: relative; height: 180px; }}
    table {{ width: 100%; border-collapse: collapse; background: var(--card);
             border: 1px solid var(--border); border-radius: 6px; overflow: hidden;
             font-size: 0.8rem; }}
    th, td {{ padding: 0.35rem 0.5rem; text-align: left; }}
    th {{ background: #f9fafb; font-size: 0.7rem; text-transform: uppercase;
         letter-spacing: 0.04em; color: var(--muted); font-weight: 600; }}
    td {{ border-top: 1px solid var(--border); }}
    .node-name {{ font-family: ui-monospace, monospace; }}
    .status {{ font-weight: 600; }}
    .status-success, .status-pass {{ color: var(--ok); }}
    .status-fail, .status-error {{ color: var(--fail); }}
    .status-skip {{ color: var(--skip); }}
    .row-fail {{ background: #fef2f2; }}
    .row-skip {{ opacity: 0.85; }}
  </style>
</head>
<body>
  <div class="container">
    <h1>DBT Run Report</h1>
    <div class="summary">
      <strong>Run summary</strong>
      <div class="summary-grid">
        <div><strong>{total}</strong><br/><span class="badge">Total nodes</span></div>
        <div><strong>{ok}</strong><br/><span class="badge badge-ok">Passed</span></div>
        <div><strong>{bad}</strong><br/><span class="badge badge-fail">Failed</span></div>
        <div><strong>{skipped}</strong><br/><span class="badge badge-skip">Skipped</span></div>
        <div><strong>{elapsed}</strong><br/><span class="badge">Duration</span></div>
      </div>
      <div class="overall {overall_class}">Overall: {overall}</div>
      <div class="meta">Command: {command} &nbsp;|&nbsp; Generated: {generated}</div>
    </div>

    <div class="section">
      <h2>Failures</h2>
      <table>
        <thead>
          <tr>
            <th>Type</th>
            <th>Node</th>
            <th>Status</th>
            <th>Time</th>
            <th>Rows</th>
            <th>Failures</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
{failures_body}
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Charts</h2>
      <div class="charts">
        <div class="chart-box">
          <h3>Status</h3>
          <div class="chart-wrap"><canvas id="chartStatus"></canvas></div>
        </div>
        <div class="chart-box">
          <h3>Duration by node (top 10, sec)</h3>
          <div class="chart-wrap"><canvas id="chartDuration"></canvas></div>
        </div>
      </div>
    </div>

    <div class="section">
      <h2>All nodes</h2>
      <table>
        <thead>
          <tr>
            <th>Type</th>
            <th>Node</th>
            <th>Status</th>
            <th>Time</th>
            <th>Rows</th>
            <th>Failures</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
{all_body}
        </tbody>
      </table>
    </div>
  </div>
  <script>
    var d = {chart_data};
    var statusCtx = document.getElementById('chartStatus').getContext('2d');
    new Chart(statusCtx, {{
      type: 'doughnut',
      data: {{
        labels: ['Passed', 'Failed', 'Skipped'],
        datasets: [{{
          data: [d.passed, d.failed, d.skipped],
          backgroundColor: ['#059669', '#dc2626', '#9ca3af'],
          borderWidth: 0
        }}]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{ legend: {{ position: 'bottom' }} }}
      }}
    }});
    var durCtx = document.getElementById('chartDuration').getContext('2d');
    new Chart(durCtx, {{
      type: 'bar',
      data: {{
        labels: d.durationLabels,
        datasets: [{{ label: 'Seconds', data: d.durationValues, backgroundColor: '#2563eb' }}]
      }},
      options: {{
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        scales: {{ x: {{ beginAtZero: true }} }},
        plugins: {{ legend: {{ display: false }} }}
      }}
    }});
  </script>
</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a user-friendly report from dbt run_results.json"
    )
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="dbt project root (default: auto-detect from cwd)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="HTML report path (default: target/dbt_run_report.html)",
    )
    parser.add_argument(
        "--no-html",
        action="store_true",
        help="Only print console report, do not write HTML",
    )
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Only write HTML, do not print console report",
    )
    args = parser.parse_args()

    project_dir = args.project_dir or find_project_dir()
    if not project_dir:
        print("Error: Could not find dbt project (dbt_project.yml). Specify --project-dir.", file=sys.stderr)
        return 1

    report = build_report_data(project_dir)
    if not report and not args.no_console:
        print("No run results found. Run dbt first (e.g. dbt run, dbt build, dbt test).", file=sys.stderr)
        return 1

    project_name = "dbt"
    dbt_project_path = project_dir / "dbt_project.yml"
    if dbt_project_path.exists():
        try:
            with open(dbt_project_path, encoding="utf-8") as f:
                for line in f:
                    if line.strip().startswith("name:"):
                        project_name = line.split("name:")[-1].strip().strip("'\"").strip()
                        break
        except OSError:
            pass

    if not args.no_console and report:
        print_console_report(report)

    if not args.no_html and report:
        out_path = args.output or (project_dir / "target" / "dbt_run_report.html")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        html = build_html_report(report, project_name=project_name)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        if not args.no_console:
            print(f"HTML report written to: {out_path}")

    return 0 if (report and report["overall_success"]) else (1 if report else 1)


if __name__ == "__main__":
    sys.exit(main())
