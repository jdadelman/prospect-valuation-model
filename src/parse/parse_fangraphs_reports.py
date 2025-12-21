import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag


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


TOOL_CUR_FUT_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s*$")
def split_cur_fut(value: str) -> tuple[Optional[str], Optional[str]]:
    """
    Split tool strings like '20/50' into ('20', '50').
    Only matches pure numeric cur/fut. Otherwise returns (None, None).
    Output strings are normalized numeric strings (int-like if possible).
    """
    if value is None:
        return None, None
    s = str(value).strip()
    if not s:
        return None, None
    m = TOOL_CUR_FUT_RE.match(s)
    if not m:
        return None, None

    cur_s, fut_s = m.group(1), m.group(2)

    def norm_num(x: str) -> str:
        # 20.0 -> 20 ; 42.5 stays 42.5
        if "." in x:
            fx = float(x)
            if fx.is_integer():
                return str(int(fx))
            return str(fx)
        return str(int(x))

    return norm_num(cur_s), norm_num(fut_s)


def normalize_tool_fields_in_row(row: dict[str, Any]) -> dict[str, Any]:
    """
    For every key 'tool_<name>' with value 'CUR/FUT', add:
      'tool_<name>_cur', 'tool_<name>_fut'
    Keeps raw field unchanged.
    """
    out = dict(row)
    for k, v in row.items():
        if not isinstance(k, str):
            continue
        if not k.startswith("tool_"):
            continue
        if k.endswith("_cur") or k.endswith("_fut"):
            continue

        cur, fut = split_cur_fut(str(v) if v is not None else "")
        if cur is not None and fut is not None:
            out[f"{k}_cur"] = cur
            out[f"{k}_fut"] = fut
        else:
            # Keep empty explicit columns optional; writer will fill missing with ''
            pass
    return out


@dataclass(frozen=True)
class SummaryRow:
    rk: int
    player_name: str
    fgid: str
    player_url: str
    highest_level: str
    fv_raw: str


def parse_summary_table(soup: BeautifulSoup) -> tuple[dict[str, SummaryRow], dict[int, SummaryRow]]:
    table = soup.select_one("div.table-container.table-green table.sortable")
    if table is None:
        raise RuntimeError("Could not find summary table: div.table-container.table-green table.sortable")

    headers = [th.get_text(" ", strip=True) for th in table.select("thead tr th")]
    required = ["Rk", "Name", "Highest Level", "FV"]
    missing = [h for h in required if h not in headers]
    if missing:
        raise RuntimeError(f"Summary table missing headers {missing}. Found headers={headers}")

    by_fgid: dict[str, SummaryRow] = {}
    by_rk: dict[int, SummaryRow] = {}

    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        rk_text = tds[0].get_text(strip=True)
        try:
            rk = int(rk_text)
        except ValueError as e:
            raise RuntimeError(f"Could not parse rk={rk_text!r}") from e

        # Name: may be <a> or plain text
        name_a = tds[1].select_one("a[href]")
        if name_a:
            player_name = name_a.get_text(" ", strip=True)
            player_url = name_a["href"].strip()
            fgid = parse_fgid_from_player_url(player_url)
        else:
            player_name = tds[1].get_text(" ", strip=True)
            player_url = ""
            fgid = ""  # edge case: no FGID

        highest_level = tds[3].get_text(" ", strip=True)
        fv_raw = tds[6].get_text(" ", strip=True)

        row = SummaryRow(
            rk=rk,
            player_name=player_name,
            fgid=fgid,
            player_url=player_url,
            highest_level=highest_level,
            fv_raw=fv_raw,
        )

        if rk in by_rk:
            raise RuntimeError(f"Duplicate rank in summary table: rk={rk}")

        by_rk[rk] = row
        if fgid:
            if fgid in by_fgid:
                raise RuntimeError(f"Duplicate FGID in summary table: fgid={fgid}")
            by_fgid[fgid] = row

    if not by_rk:
        raise RuntimeError("Parsed zero rows from summary table.")

    # Rank sanity: 1..N
    rks = sorted(by_rk.keys())
    if rks != list(range(1, len(rks) + 1)):
        raise RuntimeError(f"Ranks not 1..N. Parsed ranks={rks}")

    return by_fgid, by_rk


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


def parse_rank_from_header_text(header_text: str) -> Optional[int]:
    # Typical: "1. Jackson Holliday, SS"
    m = re.match(r"^\s*(\d+)\.", header_text.strip())
    return int(m.group(1)) if m else None


def parse_report_blocks(
    soup: BeautifulSoup,
    summary_by_fgid: dict[str, SummaryRow],
    summary_by_rk: dict[int, SummaryRow],
) -> tuple[list[ReportRow], list[dict[str, Any]]]:
    container = soup.select_one("div.fullpostentry")
    if container is None:
        raise RuntimeError("Could not find article body container: div.fullpostentry")

    tool_items = container.select("div.tool-item.top-prospects-tool")
    if not tool_items:
        raise RuntimeError("Could not find any player tool-item blocks: div.tool-item.top-prospects-tool")

    report_rows: list[ReportRow] = []
    tools_rows: list[dict[str, Any]] = []

    # Used only if rank cannot be extracted and no FGID exists:
    summary_rows_in_order = [summary_by_rk[i] for i in range(1, len(summary_by_rk) + 1)]
    used_rks: set[int] = set()

    for idx, item in enumerate(tool_items):
        header_h3 = item.select_one("div.table-header.grey h3.header-name")
        if header_h3 is None:
            raise RuntimeError("Tool-item missing h3.header-name")

        header_text = header_h3.get_text(" ", strip=True)
        rk_from_header = parse_rank_from_header_text(header_text)

        header_a = item.select_one("div.table-header.grey a[href*='playerid=']")
        fgid = ""
        player_url_from_block = ""
        if header_a is not None:
            player_url_from_block = header_a["href"].strip()
            try:
                fgid = parse_fgid_from_player_url(player_url_from_block)
            except Exception:
                fgid = ""

        # Choose summary row: FGID > rank > positional fallback
        srow: Optional[SummaryRow] = None

        if fgid and fgid in summary_by_fgid:
            srow = summary_by_fgid[fgid]
        elif rk_from_header is not None and rk_from_header in summary_by_rk:
            srow = summary_by_rk[rk_from_header]
        else:
            # Positional fallback (least reliable)
            if idx < len(summary_rows_in_order):
                srow = summary_rows_in_order[idx]

        if srow is None:
            raise RuntimeError("Could not match report block to any summary row.")

        if srow.rk in used_rks:
            raise RuntimeError(f"Matched multiple report blocks to same rank: rk={srow.rk}")
        used_rks.add(srow.rk)

        scouting_text_raw = extract_scouting_text(item)

        report_rows.append(
            ReportRow(
                rk=srow.rk,
                fgid=srow.fgid,  # may be empty
                player_name=srow.player_name,
                highest_level=srow.highest_level,
                fv_raw=srow.fv_raw,
                scouting_text_raw=scouting_text_raw,
                player_url=srow.player_url,  # may be empty
            )
        )

        tools_dict: dict[str, Any] = {"rk": srow.rk, "fgid": srow.fgid}

        tables = item.find_all("table")
        if tables:
            meta = parse_kv_table(tables[0])
            for k in ["bat_thr", "height", "weight", "age", "fv"]:
                if k in meta:
                    tools_dict[f"meta_{k}"] = meta[k]

        for t in tables[1:]:
            parsed = parse_header_value_table(t)
            if not parsed:
                continue
            for k, v in parsed.items():
                tools_dict[f"tool_{k}"] = v

        tools_rows.append(normalize_tool_fields_in_row(tools_dict))


    # Invariants (updated)
    if len(report_rows) != len(summary_by_rk):
        raise RuntimeError(
            f"Join incomplete: summary rows={len(summary_by_rk)} but report blocks parsed={len(report_rows)}"
        )

    report_rows_sorted = sorted(report_rows, key=lambda r: r.rk)
    if [r.rk for r in report_rows_sorted] != list(range(1, len(report_rows_sorted) + 1)):
        raise RuntimeError("Report ranks are not 1..N after parsing.")

    nontrivial = sum(1 for r in report_rows_sorted if len(r.scouting_text_raw) >= 50)
    if nontrivial < int(0.8 * len(report_rows_sorted)):
        raise RuntimeError(
            f"Scouting text looks too empty: only {nontrivial}/{len(report_rows_sorted)} have >=50 chars"
        )

    return report_rows_sorted, tools_rows


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

    summary_by_fgid, summary_by_rk = parse_summary_table(soup)
    report_rows, tools_rows = parse_report_blocks(soup, summary_by_fgid, summary_by_rk)

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
