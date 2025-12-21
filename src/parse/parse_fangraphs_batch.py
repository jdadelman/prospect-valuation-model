import argparse
import csv
import sys
import traceback
from bs4 import BeautifulSoup
from pathlib import Path

# Ensure repo root is on sys.path so we can import src.parse.*
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.parse.parse_fangraphs_reports import (
    extract_canonical_url,
    extract_org_label,
    infer_report_year_from_url,
    parse_report_blocks,
    parse_summary_table,
    write_reports_csv,
    write_tools_csv,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch parse FanGraphs org-year HTML files into intermediate CSVs.")
    ap.add_argument(
        "--indir",
        default="data/raw/fangraphs/org_year_pages",
        help="Directory containing saved org-year HTML files",
    )
    ap.add_argument(
        "--outdir",
        default="data/intermediate/fangraphs",
        help="Directory for intermediate outputs",
    )
    ap.add_argument(
        "--manifest",
        default="data/intermediate/fangraphs/parse_manifest.csv",
        help="Manifest CSV path",
    )
    args = ap.parse_args()

    indir = Path(args.indir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    html_files = sorted(indir.glob("*.html"))
    if not html_files:
        raise SystemExit(f"No .html files found in {indir}")

    fieldnames = [
        "html_path",
        "slug",
        "source_url",
        "org_label",
        "report_year",
        "summary_rows",
        "report_blocks",
        "reports_csv",
        "tools_csv",
        "ok",
        "error",
    ]

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for i, html_path in enumerate(html_files, start=1):
            slug = html_path.stem
            row = {
                "html_path": str(html_path.as_posix()),
                "slug": slug,
                "source_url": "",
                "org_label": "",
                "report_year": "",
                "summary_rows": "",
                "report_blocks": "",
                "reports_csv": "",
                "tools_csv": "",
                "ok": "0",
                "error": "",
            }

            try:
                html = html_path.read_text(encoding="utf-8", errors="replace")
                soup = BeautifulSoup(html, "lxml")

                source_url = extract_canonical_url(soup)
                org_label = extract_org_label(soup)
                report_year = infer_report_year_from_url(source_url) if source_url else None
                if report_year is None:
                    raise RuntimeError("Could not infer report_year from canonical URL.")

                summary_by_fgid, summary_by_rk = parse_summary_table(soup)
                report_rows, tools_rows = parse_report_blocks(soup, summary_by_fgid, summary_by_rk)

                row["summary_rows"] = str(len(summary_by_rk))
                row["report_blocks"] = str(len(report_rows))

                reports_out = outdir / f"reports_{slug}.csv"
                tools_out = outdir / f"tools_{slug}.csv"

                write_reports_csv(
                    report_rows,
                    reports_out,
                    org_label=org_label,
                    report_year=report_year,
                    source_url=source_url,
                )
                write_tools_csv(tools_rows, tools_out)

                row["source_url"] = source_url
                row["org_label"] = org_label
                row["report_year"] = str(report_year)
                row["summary_rows"] = str(len(summary_by_fgid))
                row["report_blocks"] = str(len(report_rows))
                row["reports_csv"] = str(reports_out.as_posix())
                row["tools_csv"] = str(tools_out.as_posix())
                row["ok"] = "1"

            except Exception as e:
                row["error"] = f"{type(e).__name__}: {e}"
                print(traceback.format_exc(), file=sys.stderr)

            w.writerow(row)
            print(f"[{i}/{len(html_files)}] {html_path.name} -> ok={row['ok']} {row['error']}".strip())

    print(f"[OK] Wrote parse manifest: {manifest_path}")


if __name__ == "__main__":
    main()
