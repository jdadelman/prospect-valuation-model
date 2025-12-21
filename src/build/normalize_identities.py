import argparse
import csv
import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Iterable, Optional


def norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())


def normalize_name_for_match(name: str) -> str:
    """
    Normalize a player name for matching (casefold, remove punctuation-ish, collapse spaces).
    This is not intended to be reversible; it's for stable comparison keys.
    """
    s = norm_space(name).casefold()
    # drop periods/commas/apostrophes; keep spaces and letters
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def split_first_last(name: str) -> tuple[str, str]:
    """
    Best-effort split. Not authoritative (e.g., compound surnames, suffixes).
    Intended only as helpful metadata for downstream matching.
    """
    s = norm_space(name)
    if not s:
        return "", ""
    parts = s.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def stable_fallback_id(player_name: str, player_url: str) -> str:
    """
    For rows with missing FGID, emit a stable synthetic key so we can dedupe consistently.
    This is NOT a real-world identifier; it is only for tracking unresolved identities.
    """
    base = f"{norm_space(player_name)}|{norm_space(player_url)}"
    h = hashlib.sha256(base.encode("utf-8")).hexdigest()[:12]
    return f"no_fgid_{h}"


# FanGraphs org_label is free text like:
# "Baltimore Orioles Top 45 Prospects"
MLB_ORG_ABBREV_BY_NAME = {
    "arizona diamondbacks": "ARI",
    "atlanta braves": "ATL",
    "baltimore orioles": "BAL",
    "boston red sox": "BOS",
    "chicago cubs": "CHC",
    "chicago white sox": "CWS",
    "cincinnati reds": "CIN",
    "cleveland guardians": "CLE",
    "colorado rockies": "COL",
    "detroit tigers": "DET",
    "houston astros": "HOU",
    "kansas city royals": "KC",
    "los angeles angels": "LAA",
    "los angeles dodgers": "LAD",
    "miami marlins": "MIA",
    "milwaukee brewers": "MIL",
    "minnesota twins": "MIN",
    "new york mets": "NYM",
    "new york yankees": "NYY",
    "oakland athletics": "OAK",
    "philadelphia phillies": "PHI",
    "pittsburgh pirates": "PIT",
    "san diego padres": "SD",
    "san francisco giants": "SF",
    "seattle mariners": "SEA",
    "st louis cardinals": "STL",
    "st. louis cardinals": "STL",
    "tampa bay rays": "TB",
    "texas rangers": "TEX",
    "toronto blue jays": "TOR",
    "washington nationals": "WSH",
    # historical FanGraphs label variant
    "cleveland baseball team": "CLE",
}


def org_abbrev_from_org_label(org_label: str) -> str:
    """
    Extract MLB org abbreviation from FanGraphs org_label string.
    Best-effort contains-match on normalized org names.
    """
    s = normalize_name_for_match(org_label)
    for name_norm, ab in MLB_ORG_ABBREV_BY_NAME.items():
        if name_norm in s:
            return ab
    return ""


@dataclass
class IdentityAgg:
    identity_key: str
    fgid: str
    player_name: str
    player_url: str
    name_norm: str
    first_name: str
    last_name: str
    report_years: set[int]
    org_labels: set[str]
    org_abbrevs: set[str]
    published_dates: set[str]
    ages: list[float]
    source_files: set[str]


def iter_report_csvs(indir: Path) -> Iterable[Path]:
    # Only use reports_*.csv as the authoritative source of names/urls.
    yield from sorted(indir.glob("reports_*.csv"))


def iter_tools_csvs(indir: Path) -> Iterable[Path]:
    yield from sorted(indir.glob("tools_*.csv"))


def read_reports_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {"rk", "fgid", "player_name", "player_url", "org_label", "report_year", "published_date"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise RuntimeError(f"{path}: missing required columns: {sorted(missing)}")
        for row in r:
            yield {k: (row.get(k) or "").strip() for k in required}


def read_tools_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return
        required = {"rk", "fgid"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise RuntimeError(f"{path}: missing required columns: {sorted(missing)}")

        has_meta_age = "meta_age" in (r.fieldnames or [])
        for row in r:
            out = {
                "rk": (row.get("rk") or "").strip(),
                "fgid": (row.get("fgid") or "").strip(),
                "meta_age": (row.get("meta_age") or "").strip() if has_meta_age else "",
            }
            yield out


def parse_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def parse_float(s: str) -> Optional[float]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def build_identities(reports_dir: Path) -> list[IdentityAgg]:
    by_key: dict[str, IdentityAgg] = {}

    # Build auxiliary index from tools_*.csv: fgid -> list of meta_age floats
    age_by_fgid: dict[str, list[float]] = defaultdict(list)
    for tools_path in iter_tools_csvs(reports_dir):
        for row in read_tools_rows(tools_path):
            fgid = row.get("fgid", "").strip()
            if not fgid:
                continue
            age = parse_float(row.get("meta_age", ""))
            if age is not None:
                age_by_fgid[fgid].append(age)

    for csv_path in iter_report_csvs(reports_dir):
        for row in read_reports_rows(csv_path):
            fgid = row["fgid"].strip()
            player_name = norm_space(row["player_name"])
            player_url = norm_space(row["player_url"])
            org_label = norm_space(row["org_label"])
            published_date = norm_space(row["published_date"])
            ry = parse_int(row["report_year"])

            if fgid:
                identity_key = fgid
            else:
                identity_key = stable_fallback_id(player_name, player_url)

            if identity_key not in by_key:
                first, last = split_first_last(player_name)
                by_key[identity_key] = IdentityAgg(
                    identity_key=identity_key,
                    fgid=fgid,
                    player_name=player_name,
                    player_url=player_url,
                    name_norm=normalize_name_for_match(player_name),
                    first_name=first,
                    last_name=last,
                    report_years=set(),
                    org_labels=set(),
                    org_abbrevs=set(),
                    published_dates=set(),
                    ages=[],
                    source_files=set(),
                )

            agg = by_key[identity_key]

            # If FGID exists, it should be consistent for that identity_key
            if agg.fgid and fgid and agg.fgid != fgid:
                raise RuntimeError(f"Inconsistent FGID for key={identity_key}: {agg.fgid} vs {fgid}")

            # Prefer non-empty name/url if some rows are empty (should be rare)
            if not agg.player_name and player_name:
                agg.player_name = player_name
                agg.name_norm = normalize_name_for_match(player_name)
                agg.first_name, agg.last_name = split_first_last(player_name)

            if not agg.player_url and player_url:
                agg.player_url = player_url

            if ry is not None:
                agg.report_years.add(ry)

            if org_label:
                agg.org_labels.add(org_label)
                ab = org_abbrev_from_org_label(org_label)
                if ab:
                    agg.org_abbrevs.add(ab)

            if published_date:
                agg.published_dates.add(published_date)

            # Attach ages from tools index (FGID-only; if FGID missing, we cannot attach)
            if agg.fgid:
                agg.ages.extend(age_by_fgid.get(agg.fgid, []))

            agg.source_files.add(csv_path.name)

    # Deterministic order: real FGIDs first, then synthetic keys
    def sort_key(a: IdentityAgg) -> tuple[int, str]:
        is_synth = 1 if a.identity_key.startswith("no_fgid_") else 0
        return (is_synth, a.identity_key)

    return sorted(by_key.values(), key=sort_key)


def write_identities(identities: list[IdentityAgg], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "identity_key",
        "fgid",
        "player_name",
        "player_url",
        "name_norm",
        "first_name",
        "last_name",
        "report_years",
        "org_labels",
        "org_abbrevs",
        "published_date_latest",
        "age_mean",
        "age_samples",
        "source_files_count",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for a in identities:
            published_latest = max(a.published_dates) if a.published_dates else ""
            age_mean = f"{mean(a.ages):.3f}" if a.ages else ""
            age_samples = "|".join(f"{x:.3f}" for x in sorted(a.ages)) if a.ages else ""

            w.writerow(
                {
                    "identity_key": a.identity_key,
                    "fgid": a.fgid,
                    "player_name": a.player_name,
                    "player_url": a.player_url,
                    "name_norm": a.name_norm,
                    "first_name": a.first_name,
                    "last_name": a.last_name,
                    "report_years": ",".join(str(x) for x in sorted(a.report_years)),
                    "org_labels": " | ".join(sorted(a.org_labels)),
                    "org_abbrevs": "|".join(sorted(a.org_abbrevs)),
                    "published_date_latest": published_latest,
                    "age_mean": age_mean,
                    "age_samples": age_samples,
                    "source_files_count": str(len(a.source_files)),
                }
            )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Normalize player identities from intermediate FanGraphs reports CSVs into a canonical list."
    )
    ap.add_argument(
        "--reports-dir",
        default="data/intermediate/fangraphs",
        help="Directory containing reports_*.csv and tools_*.csv files",
    )
    ap.add_argument(
        "--out",
        default="data/processed/player_identities.csv",
        help="Output path for canonical identities CSV",
    )
    args = ap.parse_args()

    reports_dir = Path(args.reports_dir)
    if not reports_dir.exists():
        raise SystemExit(f"reports-dir not found: {reports_dir}")

    identities = build_identities(reports_dir)
    out_path = Path(args.out)
    write_identities(identities, out_path)

    n_total = len(identities)
    n_missing_fgid = sum(1 for a in identities if not a.fgid)
    n_with_org = sum(1 for a in identities if a.org_abbrevs)
    n_with_age = sum(1 for a in identities if a.ages)
    n_with_pub = sum(1 for a in identities if a.published_dates)

    print(f"[OK] Wrote {n_total} identities -> {out_path}")
    print(f"     Missing FGID: {n_missing_fgid} ({(n_missing_fgid / max(n_total, 1)):.1%})")
    print(f"     With org abbrev: {n_with_org} ({(n_with_org / max(n_total, 1)):.1%})")
    print(f"     With age samples: {n_with_age} ({(n_with_age / max(n_total, 1)):.1%})")
    print(f"     With published_date: {n_with_pub} ({(n_with_pub / max(n_total, 1)):.1%})")


if __name__ == "__main__":
    main()
