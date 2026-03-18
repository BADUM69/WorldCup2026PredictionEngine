"""
run_quality_report.py — Story 1.4: Data Quality Report

Runs all checks from utils/quality_checks.py against the live database
and produces two outputs:
    - logs/quality_report_YYYYMMDD.txt   (plain text summary)
    - outputs/quality_report_YYYYMMDD.md (markdown, paste into docs)

Run any time after data has been loaded:
    python run_quality_report.py

Exit codes:
    0 — all checks passed
    1 — one or more checks failed
    2 — one or more warnings (no failures)
"""
import sys
import os
import logging
from datetime import datetime

from utils.quality_checks import CHECKS
from config import LOGS_DIR

log = logging.getLogger("quality_report")

KNOWN_GAPS = """
## Known data gaps (expected, not errors)

- Pre-1966 match statistics (possession, shots, cards) are largely unavailable
  from all sources. match_stats coverage below ~60% is expected and acceptable.
- Player date of birth is frequently missing for tournaments before 1970.
  Transfermarkt coverage improves significantly from 1982 onwards.
- Tournament squad lists (tournament_squads) are only available from sources
  covering 2006 onwards without significant manual effort. Earlier squads
  should be treated as best-effort.
- Own goals are not attributed to individual players in most sources, which
  causes a small discrepancy between match scorelines and summed player goals.
  A tolerance of 2 goals per match is applied in the quality check.
- StatsBomb event data covers 2018 and 2022 only. player_match_stats for
  earlier tournaments will have fewer fields populated (no distance_km, sprints).
"""


def run_all_checks() -> list[dict]:
    results = []
    for check_fn in CHECKS:
        try:
            result = check_fn()
            results.append(result)
            status_sym = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}[result["status"]]
            log.info("[%s] %s — %s", status_sym, result["name"], result["detail"])
        except Exception as e:
            log.error("Check %s crashed: %s", check_fn.__name__, e)
            results.append({
                "name":      check_fn.__name__,
                "table":     "unknown",
                "status":    "fail",
                "value":     None,
                "threshold": None,
                "detail":    f"Check crashed: {e}",
            })
    return results


def summary_stats(results: list[dict]) -> tuple[int, int, int]:
    passed  = sum(1 for r in results if r["status"] == "pass")
    warned  = sum(1 for r in results if r["status"] == "warn")
    failed  = sum(1 for r in results if r["status"] == "fail")
    return passed, warned, failed


def render_markdown(results: list[dict], timestamp: str) -> str:
    passed, warned, failed = summary_stats(results)
    total = len(results)

    lines = [
        "# World Cup 2026 Prediction Engine",
        "## Story 1.4 — Data Quality Report",
        f"Generated: {timestamp}",
        "",
        "---",
        "",
        "## Summary",
        "",
        f"| Result | Count |",
        f"|--------|-------|",
        f"| Pass   | {passed} |",
        f"| Warn   | {warned} |",
        f"| Fail   | {failed} |",
        f"| Total  | {total} |",
        "",
    ]

    if failed > 0:
        lines += [
            "**Status: FAIL** — one or more critical checks did not pass.",
            "Address all FAIL items before handing data to Epic 4.",
            "",
        ]
    elif warned > 0:
        lines += [
            "**Status: WARN** — all critical checks passed, some warnings to review.",
            "",
        ]
    else:
        lines += ["**Status: PASS** — all checks passed.", ""]

    # Group by table
    tables = {}
    for r in results:
        tables.setdefault(r["table"], []).append(r)

    lines.append("---")
    lines.append("")
    lines.append("## Results by table")
    lines.append("")

    STATUS_ICON = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}

    for table, checks in tables.items():
        lines.append(f"### {table}")
        lines.append("")
        lines.append("| Status | Check | Detail |")
        lines.append("|--------|-------|--------|")
        for r in checks:
            icon   = STATUS_ICON[r["status"]]
            detail = r["detail"].replace("|", "-")
            lines.append(f"| {icon} | {r['name']} | {detail} |")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines += KNOWN_GAPS.strip().split("\n")

    return "\n".join(lines)


def render_text(results: list[dict], timestamp: str) -> str:
    passed, warned, failed = summary_stats(results)
    lines = [
        "=" * 70,
        "WORLD CUP 2026 — STORY 1.4 DATA QUALITY REPORT",
        f"Generated: {timestamp}",
        "=" * 70,
        "",
        f"  PASS: {passed}   WARN: {warned}   FAIL: {failed}   TOTAL: {len(results)}",
        "",
        "-" * 70,
    ]
    for r in results:
        sym = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}[r["status"]]
        lines.append(f"[{sym}]  {r['name']}")
        lines.append(f"       {r['detail']}")
        lines.append("")

    lines += ["-" * 70, "", KNOWN_GAPS]
    return "\n".join(lines)


def main():
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs("outputs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stamp     = datetime.now().strftime("%Y%m%d_%H%M%S")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    log.info("Running Story 1.4 quality checks...")
    results = run_all_checks()
    passed, warned, failed = summary_stats(results)

    # Write text report
    txt_path = os.path.join(LOGS_DIR, f"quality_report_{stamp}.txt")
    with open(txt_path, "w") as f:
        f.write(render_text(results, timestamp))
    log.info("Text report written to %s", txt_path)

    # Write markdown report
    md_path = os.path.join("outputs", f"quality_report_{stamp}.md")
    with open(md_path, "w") as f:
        f.write(render_markdown(results, timestamp))
    log.info("Markdown report written to %s", md_path)

    log.info("Done — PASS: %d  WARN: %d  FAIL: %d", passed, warned, failed)

    if failed > 0:
        sys.exit(1)
    elif warned > 0:
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
