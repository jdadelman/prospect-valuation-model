import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup


@dataclass(frozen=True)
class SummaryRow:
    rk: int
    player_name: str
    fgid: str
    player_url: str
    highest_level: str
    fv_raw: str
    org_label: str
    report_year: int
    source_url: str


def extract_canonical_url(soup: BeautifulSoup) -> Optional[str]:
    # Prefer <link rel="canonical" ...> if present
    link = soup.find("link", attrs={"rel": "canonical"})
    if link and link.get("href"):
        return link["href"].strip()
    # Fallback: h1 post title anchor
    h1 = soup.select_one("h1.posttitle a[href]")
    if h1 and h1.get("href"):
        return h1["href"].strip()
    return None


def infer_report_year_from_url(url: str) -> Optional[int]:
    # Typical slug ends with "...-2024/"
    m = re.search(r"-(20\d{2})/?$", url.rstrip("/"))
    if not m:
        return None
    return int(m.group(1))


def extract_org_label(soup: BeautifulSoup) -> str:
    # From the summary table title, e.g. "Orioles Top Prospects"
    title = soup.select_one("div.table-container.table-green div.table-title")
    if title:
        return " ".join(title.get_text(" ", strip=True).split())
    return ""


def parse_fgid_from_player_url(player_url: str) -> str:
    """
    Player URL examples:
      https://www.fangraphs.com/statss.aspx?playerid=31781
      https://www.fangraphs.com/statss.aspx?playerid=sa3015716
    We treat the query param value (including 'sa' prefix if present) as FGID.
    """
    parsed = urlparse(player_url)
    qs = parse_qs(parsed.query)
    pid = qs.get("playerid", [None])[0]
    if not pid:
        raise ValueError(f"Could not parse FGID from player_url={player_url}")
    return pid.strip()


def parse_summary_table(html_path: Path) -> list[SummaryRow]:
    html = html_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "lxml")

    table = soup.select_one("div.table-container.table-green table.sortable")
    if table is None:
        raise RuntimeError("Could not find summary table: div.table-container.table-green table.sortable")

    # Validate headers contain expected columns (order is assumed but we check presence)
    headers = [th.get_text(" ", strip=True) for th in table.select("thead tr th")]
    expected = ["Rk", "Name", "Highest Level", "FV"]
    missing = [x for x in expected if x not in headers]
    if missing:
        raise RuntimeError(f"Summary table missing expected headers {missing}. Found headers={headers}")

    source_url = extract_canonical_url(soup) or ""
    report_year = infer_report_year_from_url(source_url) if source_url else None
    if report_year is None:
        # As a fallback, try from filename
        report_year = infer_report_year_from_url(html_path.name)  # might be None
    if report_year is None:
        raise RuntimeError("Could not infer report_year from canonical URL or filename.")

    org_label = extract_org_label(soup)

    rows: list[SummaryRow] = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            # defensive: skip malformed rows
            continue

        rk_text = tds[0].get_text(strip=True)
        # Name cell contains link
        name_a = tds[1].select_one("a[href]")
        if name_a is None:
            raise RuntimeError("Found a summary table row without a player link in the Name column.")

        player_name = name_a.get_text(" ", strip=True)
        player_url = name_a["href"].strip()
        fgid = parse_fgid_from_player_url(player_url)

        highest_level = tds[3].get_text(" ", strip=True)
        fv_raw = tds[6].get_text(" ", strip=True)

        # Invariants: rk parseable, fgid non-empty
        try:
            rk = int(rk_text)
        except ValueError as e:
            raise RuntimeError(f"Could not parse rank as int: rk_text={rk_text!r}") from e

        if not fgid:
            raise RuntimeError(f"Empty FGID for player {player_name} ({player_url})")

        rows.append(
            SummaryRow(
                rk=rk,
                player_name=player_name,
                fgid=fgid,
                player_url=player_url,
                highest_level=highest_level,
                fv_raw=fv_raw,
                org_label=org_label,
                report_year=report_year,
                source_url=source_url,
            )
        )

    # Strong invariants (fail fast)
    if not rows:
        raise RuntimeError("Parsed zero rows from summary table.")
    # Rank monotonicity: 1..N without duplicates
    rks = [r.rk for r in rows]
    if sorted(rks) != list(range(1, len(rows) + 1)):
        raise RuntimeError(f"Ranks are not 1..N in order. Parsed ranks={rks}")

    # FGID uniqueness within page
    fgids = [r.fgid for r in rows]
    if len(set(fgids)) != len(fgids):
        raise RuntimeError("Duplicate FGIDs detected within the summary table page.")

    return rows


def write_csv(rows: list[SummaryRow], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rk",
        "player_name",
        "fgid",
        "player_url",
        "highest_level",
        "fv_raw",
        "org_label",
        "report_year",
        "source_url",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "rk": r.rk,
                    "player_name": r.player_name,
                    "fgid": r.fgid,
                    "player_url": r.player_url,
                    "highest_level": r.highest_level,
                    "fv_raw": r.fv_raw,
                    "org_label": r.org_label,
                    "report_year": r.report_year,
                    "source_url": r.source_url,
                }
            )


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse FanGraphs org-year summary table to intermediate CSV.")
    ap.add_argument("--html", required=True, help="Path to saved org-year HTML file")
    ap.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: data/intermediate/fangraphs/summary_<year>.csv)",
    )
    args = ap.parse_args()

    html_path = Path(args.html)
    rows = parse_summary_table(html_path)

    if args.out:
        out_path = Path(args.out)
    else:
        # minimal default: year only; you can change to include org later
        out_path = Path("data/intermediate/fangraphs") / f"summary_{rows[0].report_year}.csv"

    write_csv(rows, out_path)
    print(f"[OK] Parsed {len(rows)} rows -> {out_path}")


if __name__ == "__main__":
    main()
