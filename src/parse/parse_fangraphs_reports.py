import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag


# -----------------------------
# Helpers
# -----------------------------

def sanitize_key(s: str) -> str:
    """
    Convert tool header strings into stable column keys.
    Examples:
      "Raw Power" -> "raw_power"
      "Bat / Thr" -> "bat_thr"
      "S..." -> "s"
      "91-93 / 95" is a value, not a key.
    """
    s = s.strip().lower()
    s = s.replace("/", " ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def parse_fgid_from_url(player_url: str) -> str:
    parsed = urlparse(player_url)
    pid = parse_qs(parsed.query).get("playerid", [None])[0]
    if not pid:
        raise ValueError(f"Could not parse FGID from url={player_url}")
    return pid.strip()


def infer_report_year_from_url(url: str) -> Optional[int]:
    m = re.search(r"-(20\d{2})/?$", url.rstrip("/"))
    return int(m.group(1)) if m else None


def extract_canonical_url(soup: BeautifulSoup) -> str:
    link = soup.find("link", attrs={"rel": "canonical"})
    if link and link.get("href"):
        return link["href"].strip()
    h1 = soup.select_one("h1.posttitle a[href]")
    if h1 and h1.get("href"):
        return h1["href"].strip()
    return ""


def extract_org_label(soup: BeautifulSoup) -> str:
    title = soup.select_one("div.table-container.table-green div.table-title")
    if title:
        return " ".join(title.get_text(" ", strip=True).split())
    return ""


# -----------------------------
# Summary table (Phase 1) parse
# -----------------------------

@dataclass(frozen=True)
class SummaryRow:
    rk: int
    player_name: str
    fgid: str
    player_url: str
    highest_level: str
    fv_raw: str


def parse_summary_table(soup: BeautifulSoup) -> dict[str, SummaryRow]:
    table = soup.select_one("div.table-container.table-green table.sortable")
    if table is None:
        raise RuntimeError("Could not find summary table: div.table-container.table-green table.sortable")

    headers = [th.get_text(" ", strip=True) for th in table.select("thead tr th")]
    required = ["Rk", "Name", "Highest Level", "FV"]
    missing = [h for h in required if h not in headers]
    if missing:
        raise RuntimeError(f"Summary table missing headers {missing}. Found headers={headers}")

    rows_by_fgid: dict[str, SummaryRow] = {}

    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        rk_text = tds[0].get_text(strip=True)
        name_a = tds[1].select_one("a[href]")
        if not name_a:
            raise RuntimeError("Summary row missing player link in Name column.")

        player_name = name_a.get_text(" ", strip=True)
        player_url = name_a["href"].strip()
        fgid = parse_fgid_from_url(player_url)

        highest_level = tds[3].get_text(" ", strip=True)
        fv_raw = tds[6].get_text(" ", strip=True)

        try:
            rk = int(rk_text)
        except ValueError as e:
            raise RuntimeError(f"Could not parse rk={rk_text!r}") from e

        rows_by_fgid[fgid] = SummaryRow(
            rk=rk,
            player_name=player_name,
            fgid=fgid,
            player_url=player_url,
            highest_level=highest_level,
            fv_raw=fv_raw,
        )

    if not rows_by_fgid:
        raise RuntimeError("Parsed zero rows from summary table.")

    # Rank sanity: 1..N
    rks = sorted(r.rk for r in rows_by_fgid.values())
    if rks != list(range(1, len(rks) + 1)):
        raise RuntimeError(f"Ranks not 1..N. Parsed ranks={rks}")

    return rows_by_fgid


# -----------------------------
# Report block parse (Phase 2)
# -----------------------------

@dataclass(frozen=True)
class ReportRow:
    rk: int
    fgid: str
    player_name: str
    highest_level: str
    fv_raw: str
    scouting_text_raw: str
    player_url: str


def parse_kv_table(table: Tag) -> dict[str, str]:
    """
    Parse the first 'meta' table which looks like:
    Age 20.6 Height 6′ 0″ Weight 185 Bat / Thr L / R FV 70
    Implemented as a single row with alternating label/value cells.
    """
    out: dict[str, str] = {}
    cells = [c.get_text(" ", strip=True) for c in table.find_all(["th", "td"])]
    # Expect alternating label/value
    for i in range(0, len(cells) - 1, 2):
        k = sanitize_key(cells[i])
        v = cells[i + 1].strip()
        if k and v:
            out[k] = v
    return out


def parse_header_value_table(table: Tag) -> Optional[dict[str, str]]:
    """
    Parse a tool table that has a header row then one value row.
    Example hitter tools:
      Hit Raw Power ... Throw
      40/55 45/60 ... 50
    Example pitcher tools:
      Fastball Slider ... Command S...
      45/50 50/55 ... 91-93 / 95
    """
    trs = table.find_all("tr")
    if len(trs) < 2:
        return None

    header_cells = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])]
    value_cells = [c.get_text(" ", strip=True) for c in trs[1].find_all(["th", "td"])]

    header_cells = [h for h in header_cells if h]  # remove empties
    value_cells = [v for v in value_cells if v or v == "0"]

    if not header_cells:
        return None
    if len(header_cells) != len(value_cells):
        return None

    out: dict[str, str] = {}
    for h, v in zip(header_cells, value_cells):
        k = sanitize_key(h)
        if k:
            out[k] = v.strip()
    return out if out else None


def extract_scouting_text(tool_item: Tag) -> str:
    paras = tool_item.find_all("p")
    texts = []
    for p in paras:
        t = p.get_text(" ", strip=True)
        if t:
            texts.append(t)
    return "\n\n".join(texts).strip()


def parse_report_blocks(soup: BeautifulSoup, summary_by_fgid: dict[str, SummaryRow]) -> tuple[list[ReportRow], list[dict[str, Any]]]:
    """
    Returns:
      report_rows: list of per-player rows (joined to summary)
      tools_rows: list of dict rows with wide tool fields (rk, fgid, ...)
    """
    container = soup.select_one("div.fullpostentry")
    if container is None:
        raise RuntimeError("Could not find article body container: div.fullpostentry")

    tool_items = container.select("div.tool-item.top-prospects-tool")
    if not tool_items:
        raise RuntimeError("Could not find any player tool-item blocks: div.tool-item.top-prospects-tool")

    report_rows: list[ReportRow] = []
    tools_rows: list[dict[str, Any]] = []

    for item in tool_items:
        header_a = item.select_one("div.table-header.grey a[href*='playerid=']")
        header_h3 = item.select_one("div.table-header.grey h3.header-name")

        if header_a is None or header_h3 is None:
            raise RuntimeError("Tool-item missing expected header link or h3.header-name.")

        player_url = header_a["href"].strip()
        fgid = parse_fgid_from_url(player_url)

        if fgid not in summary_by_fgid:
            raise RuntimeError(f"FGID from report block not found in summary table: fgid={fgid}")

        srow = summary_by_fgid[fgid]

        scouting_text_raw = extract_scouting_text(item)

        report_rows.append(
            ReportRow(
                rk=srow.rk,
                fgid=fgid,
                player_name=srow.player_name,
                highest_level=srow.highest_level,
                fv_raw=srow.fv_raw,
                scouting_text_raw=scouting_text_raw,
                player_url=srow.player_url,
            )
        )

        # Tools: parse all tables inside the tool-item
        tools_dict: dict[str, Any] = {"rk": srow.rk, "fgid": fgid}
        tables = item.find_all("table")

        # First table is commonly the meta key/value table
        if tables:
            meta = parse_kv_table(tables[0])
            # Keep these optional fields (may be useful for debugging even if MLBAM overrides later)
            for k in ["bat_thr", "height", "weight", "age", "fv"]:
                if k in meta:
                    tools_dict[f"meta_{k}"] = meta[k]

        # Remaining tables: try header/value parsing
        for t in tables[1:]:
            parsed = parse_header_value_table(t)
            if not parsed:
                continue

            # Heuristic: hitter tools table contains hit/raw_power/game_power/run/fielding/throw
            # pitcher tools table contains command and pitches.
            # We keep everything; wide union happens at write time.
            for k, v in parsed.items():
                tools_dict[f"tool_{k}"] = v

        tools_rows.append(tools_dict)

    # Invariants
    if len(report_rows) != len(summary_by_fgid):
        # Allowing this would hide errors; fail fast.
        raise RuntimeError(
            f"Join incomplete: summary rows={len(summary_by_fgid)} but report blocks parsed={len(report_rows)}"
        )

    # Text sanity (weak but useful)
    nontrivial = sum(1 for r in report_rows if len(r.scouting_text_raw) >= 50)
    if nontrivial < int(0.8 * len(report_rows)):
        raise RuntimeError(
            f"Scouting text looks too empty: only {nontrivial}/{len(report_rows)} have >=50 chars"
        )

    # Rank sanity
    report_rows_sorted = sorted(report_rows, key=lambda r: r.rk)
    if [r.rk for r in report_rows_sorted] != list(range(1, len(report_rows_sorted) + 1)):
        raise RuntimeError("Report ranks are not 1..N after parsing.")

    return report_rows_sorted, tools_rows


# -----------------------------
# Writers
# -----------------------------

def write_reports_csv(report_rows: list[ReportRow], out_path: Path, org_label: str, report_year: int, source_url: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rk",
        "fgid",
        "player_name",
        "highest_level",
        "fv_raw",
        "scouting_text_raw",
        "player_url",
        "org_label",
        "report_year",
        "source_url",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in report_rows:
            w.writerow(
                {
                    "rk": r.rk,
                    "fgid": r.fgid,
                    "player_name": r.player_name,
                    "highest_level": r.highest_level,
                    "fv_raw": r.fv_raw,
                    "scouting_text_raw": r.scouting_text_raw,
                    "player_url": r.player_url,
                    "org_label": org_label,
                    "report_year": report_year,
                    "source_url": source_url,
                }
            )


def write_tools_csv(tools_rows: list[dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # wide union of all keys
    all_keys = set()
    for r in tools_rows:
        all_keys.update(r.keys())

    # Ensure stable ordering: rk, fgid first, then others sorted
    fieldnames = ["rk", "fgid"] + sorted(k for k in all_keys if k not in ("rk", "fgid"))

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in sorted(tools_rows, key=lambda x: int(x["rk"])):
            w.writerow({k: r.get(k, "") for k in fieldnames})


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Parse FanGraphs org-year reports (tools + text) to intermediate CSVs.")
    ap.add_argument("--html", required=True, help="Path to saved org-year HTML file")
    ap.add_argument(
        "--outdir",
        default="data/intermediate/fangraphs",
        help="Output directory for intermediate CSVs",
    )
    args = ap.parse_args()

    html_path = Path(args.html)
    html = html_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "lxml")

    source_url = extract_canonical_url(soup)
    report_year = infer_report_year_from_url(source_url) if source_url else None
    if report_year is None:
        raise RuntimeError("Could not infer report_year from canonical URL.")

    org_label = extract_org_label(soup)
    outdir = Path(args.outdir)

    summary_by_fgid = parse_summary_table(soup)
    report_rows, tools_rows = parse_report_blocks(soup, summary_by_fgid)

    # filename slug: prefer URL slug
    slug = re.sub(r"[?#].*$", "", (source_url or html_path.name)).rstrip("/").split("/")[-1]
    slug = slugify(slug)

    reports_out = outdir / f"reports_{slug}.csv"
    tools_out = outdir / f"tools_{slug}.csv"

    write_reports_csv(report_rows, reports_out, org_label=org_label, report_year=report_year, source_url=source_url)
    write_tools_csv(tools_rows, tools_out)

    print(f"[OK] Wrote {len(report_rows)} report rows -> {reports_out}")
    print(f"[OK] Wrote {len(tools_rows)} tool rows   -> {tools_out}")


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:120] if len(s) > 120 else s


if __name__ == "__main__":
    main()
