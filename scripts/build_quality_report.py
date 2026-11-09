"""Build a self-contained HTML data-quality and freshness report.

Inputs : the DuckDB warehouse (marts) and dbt's `target/run_results.json` (test outcomes).
Output : reports/quality_report.html   (open it in any browser, no server needed)

    python -m scripts.build_quality_report
    python -m scripts.build_quality_report --expected-date 2024-01-06   # adds a freshness verdict
"""

from __future__ import annotations

import argparse
import html
import json
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import duckdb

from ingestion.config import DBT_PROJECT_DIR, DUCKDB_PATH, REPO_ROOT

OUT_PATH = REPO_ROOT / "reports" / "quality_report.html"

CSS = """
:root { --bg:#fafaf8; --card:#fff; --ink:#1d2433; --muted:#667085; --line:#e4e7ec;
        --ok:#1a7f4b; --warn:#b54708; --bad:#b42318; --accent:#2f5fd0; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#12151c; --card:#1a1f2b; --ink:#e8ebf2; --muted:#98a2b3; --line:#2a3142;
          --ok:#4cc38a; --warn:#f0a050; --bad:#ff7b72; --accent:#7ea2ff; } }
* { box-sizing:border-box; }
body { margin:0; background:var(--bg); color:var(--ink);
       font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif; }
main { max-width:980px; margin:0 auto; padding:24px 16px 56px; }
h1 { font-size:22px; margin:0 0 4px; } h2 { font-size:16px; margin:32px 0 10px; }
.sub { color:var(--muted); margin:0 0 20px; }
.banner { padding:12px 16px; border-radius:10px; font-weight:600; margin-bottom:20px;
          border:1px solid var(--line); background:var(--card); }
.banner.ok { border-left:6px solid var(--ok); } .banner.bad { border-left:6px solid var(--bad); }
.banner.warn { border-left:6px solid var(--warn); }
.tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; }
.tile { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px 14px; }
.tile b { display:block; font-size:22px; } .tile span { color:var(--muted); font-size:13px; }
table { width:100%; border-collapse:collapse; background:var(--card);
        border:1px solid var(--line); border-radius:10px; overflow:hidden; font-size:14px; }
th, td { padding:8px 12px; text-align:left; border-bottom:1px solid var(--line); }
th { color:var(--muted); font-weight:600; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }
td.n, th.n { text-align:right; font-variant-numeric:tabular-nums; }
tr:last-child td { border-bottom:0; }
.wrap { overflow-x:auto; }
.pill { padding:1px 8px; border-radius:99px; font-size:12px; font-weight:600; border:1px solid var(--line); }
.pill.ok { color:var(--ok); } .pill.bad { color:var(--bad); } .pill.warn { color:var(--warn); }
svg text { fill:var(--muted); font-size:11px; } svg rect.bar { fill:var(--accent); }
"""


def esc(value: object) -> str:
    return html.escape(str(value))


def money(value: object) -> str:
    return f"{float(value):,.2f}"


def table(headers: list[str], rows: list[list[object]], numeric: set[int] | None = None) -> str:
    numeric = numeric or set()
    head = "".join(f'<th class="{"n" if i in numeric else ""}">{esc(h)}</th>' for i, h in enumerate(headers))
    body = "".join(
        "<tr>"
        + "".join(f'<td class="{"n" if i in numeric else ""}">{c}</td>' for i, c in enumerate(row))
        + "</tr>"
        for row in rows
    )
    return f'<div class="wrap"><table><tr>{head}</tr>{body}</table></div>'


def bar_chart(points: list[tuple[str, float]]) -> str:
    if not points:
        return "<p class='sub'>No revenue yet.</p>"
    width, height, pad_left, pad_bottom = 900, 220, 56, 26
    top = max(v for _, v in points) or 1
    slot = (width - pad_left) / len(points)
    bar_w = max(2.0, slot * 0.7)
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Recognised revenue per day" width="100%">'
    ]
    for frac in (0, 0.5, 1):
        y = (height - pad_bottom) - frac * (height - pad_bottom - 10)
        parts.append(
            f'<line x1="{pad_left}" x2="{width}" y1="{y:.1f}" y2="{y:.1f}" stroke="currentColor" opacity=".12"/>'
        )
        parts.append(f'<text x="{pad_left - 6}" y="{y + 4:.1f}" text-anchor="end">{top * frac:,.0f}</text>')
    step = max(1, len(points) // 10)
    for i, (label, value) in enumerate(points):
        h = value / top * (height - pad_bottom - 10)
        x = pad_left + i * slot + (slot - bar_w) / 2
        parts.append(
            f'<rect class="bar" x="{x:.1f}" y="{height - pad_bottom - h:.1f}" width="{bar_w:.1f}" height="{h:.1f}">'
            f"<title>{esc(label)}: {value:,.2f}</title></rect>"
        )
        if i % step == 0:
            parts.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{height - 8}" text-anchor="middle">{esc(label[5:])}</text>'
            )
    parts.append("</svg>")
    return "".join(parts)


def load_dbt_results(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text()).get("results", [])


def kind_of(unique_id: str) -> str:
    return unique_id.split(".", 1)[0]


def build_report(
    db_path: Path = DUCKDB_PATH,
    run_results_path: Path = DBT_PROJECT_DIR / "target" / "run_results.json",
    out_path: Path = OUT_PATH,
    expected_date: str | None = None,
) -> Path:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        health = con.execute("SELECT * FROM marts.mart_pipeline_health ORDER BY batch_date").fetchall()
        daily = con.execute(
            "SELECT order_date, recognised_revenue, orders, cancelled_orders "
            "FROM marts.mart_daily_revenue ORDER BY order_date"
        ).fetchall()
        reasons = con.execute(
            "SELECT reject_reason, count(*) FROM intermediate.quarantined_order_items GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        orders_total = con.execute("SELECT count(*) FROM marts.fct_orders").fetchone()[0]
        versions = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE NOT is_current) FROM marts.dim_customers"
        ).fetchone()
    finally:
        con.close()

    results = load_dbt_results(run_results_path)
    by_status = Counter(r["status"] for r in results)
    failing = [r for r in results if r["status"] in ("fail", "error")]
    warned = [r for r in results if r["status"] == "warn"]
    tests = [r for r in results if kind_of(r["unique_id"]) in ("test", "unit_test")]
    tests_ok = sum(1 for r in tests if r["status"] == "pass")

    latest_batch = health[-1][0] if health else None
    stale = bool(expected_date and (latest_batch is None or latest_batch < date.fromisoformat(expected_date)))
    if failing:
        verdict, cls = f"{len(failing)} dbt check(s) failing. Do not trust downstream numbers.", "bad"
    elif stale:
        verdict, cls = f"Stale data: latest batch {latest_batch}, expected {expected_date}.", "bad"
    elif warned:
        verdict, cls = f"Healthy with {len(warned)} warning(s).", "warn"
    elif not results:
        verdict, cls = "No dbt results found. Run `dbt build` first.", "warn"
    else:
        verdict, cls = "All checks passing. Data is fresh.", "ok"

    revenue_total = sum(float(r[1]) for r in daily)
    quarantined_total = sum(r[7] for r in health)

    kind_counts: dict[str, Counter] = {}
    for r in results:
        kind_counts.setdefault(kind_of(r["unique_id"]), Counter())[r["status"]] += 1
    dbt_rows = [
        [esc(kind), sum(c.values()), *(c.get(s, 0) for s in ("pass", "success", "warn", "fail", "error"))]
        for kind, c in sorted(kind_counts.items())
    ]

    health_rows = [
        [
            esc(h[0]),
            h[1],
            h[2],
            h[3],
            h[4],
            h[5],
            f'<span class="pill {"ok" if h[6] == 5 else "bad"}">{h[6]}/5</span>',
            h[7],
            esc(str(h[8])[:19]),
        ]
        for h in health[-14:]
    ]
    failing_html = ""
    if failing or warned:
        failing_html = "<h2>Failing or warning checks</h2>" + table(
            ["check", "status", "failures"],
            [
                [
                    esc(r["unique_id"].split(".")[-2] if r["unique_id"].count(".") > 2 else r["unique_id"]),
                    f'<span class="pill {"bad" if r["status"] in ("fail", "error") else "warn"}">{esc(r["status"])}</span>',
                    esc(r.get("failures")),
                ]
                for r in failing + warned
            ],
        )

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Data Quality Report</title><style>{CSS}</style></head><body><main>
<h1>Data quality and freshness</h1>
<p class="sub">Generated {datetime.now():%Y-%m-%d %H:%M} · latest batch {esc(latest_batch)} ·
{len(health)} day(s) loaded</p>
<div class="banner {cls}">{esc(verdict)}</div>
<div class="tiles">
 <div class="tile"><b>{tests_ok}/{len(tests)}</b><span>tests passing</span></div>
 <div class="tile"><b>{orders_total:,}</b><span>orders in fct_orders</span></div>
 <div class="tile"><b>{money(revenue_total)}</b><span>recognised revenue</span></div>
 <div class="tile"><b>{versions[0]:,}</b><span>customer versions ({versions[1]} historical)</span></div>
 <div class="tile"><b>{quarantined_total}</b><span>quarantined order lines</span></div>
</div>
<h2>Recognised revenue per day</h2>{bar_chart([(str(d[0]), float(d[1])) for d in daily])}
<h2>dbt run results</h2>
{table(["resource", "total", "pass", "success", "warn", "fail", "error"], dbt_rows, set(range(1, 7)))}
{failing_html}
<h2>Pipeline health (last 14 batch days)</h2>
{
        table(
            [
                "batch",
                "customers",
                "products",
                "orders",
                "items",
                "payments",
                "tables",
                "quarantined",
                "loaded at",
            ],
            health_rows,
            set(range(1, 8)),
        )
    }
<h2>Quarantine reasons</h2>
{
        table(["reason", "lines"], [[esc(r[0]), r[1]] for r in reasons], {1})
        if reasons
        else "<p class='sub'>Nothing quarantined.</p>"
    }
<p class="sub" style="margin-top:32px">Status counts: {esc(dict(by_status))}</p>
</main></body></html>"""

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(page, encoding="utf-8")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--expected-date", help="batch date that should be loaded by now (YYYY-MM-DD)")
    args = parser.parse_args()
    print(build_report(expected_date=args.expected_date))


if __name__ == "__main__":
    main()
