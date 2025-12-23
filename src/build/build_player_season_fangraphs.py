import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from src.utils.text import parse_int


def parse_isoish_date(s: str) -> Optional[datetime]:
    """
    Best-effort parse for strings like:
      - 2024-02-05
      - 2024-02-05T12:34:56+00:00
      - 2024-02-05T12:34:56Z
    Returns naive datetime in local parsing sense; only used for ordering.
    """
    s = (s or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1]
    try:
        return datetime.fromisoformat(s)
    except Exception:
        # fallback: date only
        try:
            return datetime.fromisoformat(s.split("T", 1)[0])
        except Exception:
            return None


def read_csv_dicts(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            raise RuntimeError(f"{path}: empty CSV / missing header")
        rows: list[dict[str, str]] = []
        for row in r:
            rows.append({k: (row.get(k) or "").strip() for k in r.fieldnames})
        return list(r.fieldnames), rows


def ensure_cols_exist(rows: list[dict[str, str]], required: set[str], path: Path) -> None:
    if not rows:
        raise RuntimeError(f"{path}: parsed zero rows")
    keys = set(rows[0].keys())
    missing = required - keys
    if missing:
        raise RuntimeError(f"{path}: missing required columns: {sorted(missing)}")


@dataclass(frozen=True)
class SeasonRow:
    identity_key: str
    fgid: str
    report_year: int
    rk: Optional[int]
    published_date: str
    row: dict[str, str]


def load_identity_map(path: Path) -> dict[str, str]:
    """
    Returns mapping: identity_key -> mlbam_id (only for rows with non-empty mlbam_id).
    If identity_key appears multiple times with different mlbam_id, error.
    """
    _, rows = read_csv_dicts(path)
    ensure_cols_exist(rows, {"identity_key", "mlbam_id"}, path)

    out: dict[str, str] = {}
    for r in rows:
        k = (r.get("identity_key") or "").strip()
        v = (r.get("mlbam_id") or "").strip()
        if not k:
            continue
        if not v:
            continue
        if k in out and out[k] != v:
            raise RuntimeError(f"{path}: conflicting mlbam_id for identity_key={k}: {out[k]} vs {v}")
        out[k] = v
    return out


def iter_season_rows(path: Path) -> Iterable[SeasonRow]:
    header, rows = read_csv_dicts(path)
    ensure_cols_exist(rows, {"identity_key", "report_year"}, path)

    for r in rows:
        identity_key = (r.get("identity_key") or "").strip()
        if not identity_key:
            continue
        ry = parse_int(r.get("report_year") or "")
        if ry is None:
            continue

        fgid = (r.get("fgid") or "").strip()
        rk = parse_int(r.get("rk") or "")
        published_date = (r.get("published_date") or "").strip()

        # Keep original row dict; project to output schema later.
        yield SeasonRow(
            identity_key=identity_key,
            fgid=fgid,
            report_year=ry,
            rk=rk,
            published_date=published_date,
            row=r,
        )


def selection_key(sr: SeasonRow) -> tuple[int, int, int, str]:
    """
    Smaller is better (because we sort ascending).
      1) has_fgid (prefer 1 -> encode as 0)
      2) published_date_latest (prefer later -> encode as negative unix-ish ordering proxy)
      3) rk (prefer smaller)
      4) identity_key (stable)
    """
    has_fgid = 0 if sr.fgid else 1  # 0 preferred
    dt = parse_isoish_date(sr.published_date)
    # For ordering, later is better; invert via negative timestamp where possible.
    if dt is not None:
        pub_ord = -int(dt.timestamp())
    else:
        pub_ord = 0  # unknown: neutral / not preferred over known-later due to has_fgid gate + rk
    rk = sr.rk if sr.rk is not None else 10**9
    return (has_fgid, pub_ord, rk, sr.identity_key)


def choose_best(rows: list[SeasonRow]) -> SeasonRow:
    if not rows:
        raise RuntimeError("choose_best called with empty rows")
    return sorted(rows, key=selection_key)[0]


def build_player_season_fangraphs(
    identity_seasons_csv: Path,
    identity_map_csv: Path,
) -> tuple[list[str], list[dict[str, str]], dict[str, int], list[str], list[dict[str, str]]]:
    id_map = load_identity_map(identity_map_csv)

    # Collect candidates per (mlbam_id, report_year)
    buckets: dict[tuple[str, int], list[SeasonRow]] = {}
    total_in = 0
    mapped = 0
    unmapped = 0

    for sr in iter_season_rows(identity_seasons_csv):
        total_in += 1
        mlbam_id = id_map.get(sr.identity_key, "")
        if not mlbam_id:
            unmapped += 1
            continue
        mapped += 1
        key = (mlbam_id, sr.report_year)
        buckets.setdefault(key, []).append(sr)

    # Deduplicate
    out_rows: list[dict[str, str]] = []
    collisions = 0
    dropped = 0
    collisions_rows: list[dict[str, str]] = []
    collisions_fieldnames = [
        "mlbam_id",
        "report_year",
        "n_candidates",
        "is_chosen",
        "selection_key",
        "identity_key",
        "fgid",
        "rk",
        "published_date",
        "source_url",
        "org_label",
        "org_abbrev",
        "player_name",
        "player_url",
    ]


    # Determine union of all columns present in identity season rows, so we can carry them through.
    # Prepend mlbam_id + report_year as canonical keys.
    union_cols: set[str] = set()
    # Compute union from the *chosen* rows to avoid columns appearing only in discarded rows.
    chosen_by_key: dict[tuple[str, int], SeasonRow] = {}

    for key, candidates in buckets.items():
        if len(candidates) > 1:
            collisions += 1
            dropped += (len(candidates) - 1)

        chosen = choose_best(candidates)
        chosen_by_key[key] = chosen
        union_cols.update(chosen.row.keys())

        if len(candidates) > 1:
            mlbam_id, report_year = key
            ordered = sorted(candidates, key=selection_key)
            n_cand = len(ordered)

            for sr in ordered:
                r = sr.row
                collisions_rows.append(
                    {
                        "mlbam_id": str(mlbam_id),
                        "report_year": str(report_year),
                        "n_candidates": str(n_cand),
                        "is_chosen": "1" if sr is chosen else "0",
                        "selection_key": str(selection_key(sr)),
                        "identity_key": sr.identity_key,
                        "fgid": sr.fgid,
                        "rk": "" if sr.rk is None else str(sr.rk),
                        "published_date": sr.published_date,
                        "source_url": (r.get("source_url") or "").strip(),
                        "org_label": (r.get("org_label") or "").strip(),
                        "org_abbrev": (r.get("org_abbrev") or "").strip(),
                        "player_name": (r.get("player_name") or "").strip(),
                        "player_url": (r.get("player_url") or "").strip(),
                    }
                )

    # Construct output schema: canonical keys first, then a stable set of carried columns.
    carried = sorted(c for c in union_cols if c not in {"identity_key"})
    output_fieldnames = ["mlbam_id", "report_year"] + carried

    for (mlbam_id, report_year), chosen in sorted(chosen_by_key.items(), key=lambda x: (int(x[0][0]) if x[0][0].isdigit() else x[0][0], x[0][1])):
        row_out: dict[str, str] = {}
        row_out["mlbam_id"] = str(mlbam_id)
        row_out["report_year"] = str(report_year)
        for c in carried:
            row_out[c] = (chosen.row.get(c) or "").strip()
        out_rows.append(row_out)

    stats = {
        "input_rows": total_in,
        "mapped_rows": mapped,
        "unmapped_rows": unmapped,
        "unique_mlbam_year_keys": len(buckets),
        "collisions_mlbam_year": collisions,
        "dropped_due_to_dedup": dropped,
        "collision_candidate_rows": len(collisions_rows),
        "output_rows": len(out_rows),
    }
    return output_fieldnames, out_rows, stats, collisions_fieldnames, collisions_rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: (r.get(k) or "") for k in fieldnames})


def write_manifest(path: Path, stats: dict[str, int], notes: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["key", "value"])
        w.writeheader()
        for k in sorted(stats.keys()):
            w.writerow({"key": k, "value": str(stats[k])})
        if notes:
            f.write("\n")
            f.write(notes.strip() + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build (mlbam_id, report_year)-keyed FanGraphs season snapshot table from identity season rows."
    )
    ap.add_argument(
        "--identity-seasons",
        default="data/processed/player_identity_seasons.csv",
        help="Input season-grain identity snapshot CSV.",
    )
    ap.add_argument(
        "--id-map",
        default="data/intermediate/identity_map_fgid_to_mlbam.csv",
        help="Input identity_key -> mlbam_id mapping CSV.",
    )
    ap.add_argument(
        "--out",
        default="data/processed/player_season_fangraphs.csv",
        help="Output (mlbam_id, report_year)-keyed FanGraphs snapshot CSV.",
    )
    ap.add_argument(
        "--manifest",
        default="data/processed/player_season_fangraphs_manifest.csv",
        help="Output manifest CSV with counts and dedupe stats.",
    )
    ap.add_argument(
        "--collisions-out",
        default="data/processed/player_season_fangraphs_collisions.csv",
        help="Output CSV listing all candidate rows for collided (mlbam_id, report_year) keys.",
    )
    args = ap.parse_args()

    identity_seasons_csv = Path(args.identity_seasons)
    id_map_csv = Path(args.id_map)
    out_csv = Path(args.out)
    manifest_csv = Path(args.manifest)

    if not identity_seasons_csv.exists():
        raise SystemExit(f"identity-seasons not found: {identity_seasons_csv}")
    if not id_map_csv.exists():
        raise SystemExit(f"id-map not found: {id_map_csv}")

    fieldnames, out_rows, stats, collisions_fieldnames, collisions_rows = build_player_season_fangraphs(
        identity_seasons_csv=identity_seasons_csv,
        identity_map_csv=id_map_csv,
    )
    write_csv(out_csv, fieldnames, out_rows)

    collisions_out = Path(args.collisions_out)
    if collisions_rows:
        write_csv(collisions_out, collisions_fieldnames, collisions_rows)

    notes = (
        "Deduping policy: prefer rows with fgid present; then later published_date; "
        "then smaller rk; then lexicographically smaller identity_key.\n"
        "This output is intended for downstream modeling keyed by (mlbam_id, report_year)."
    )
    write_manifest(manifest_csv, stats, notes=notes)

    print(f"[OK] Wrote player_season_fangraphs: {out_csv}")
    print(f"[OK] Wrote manifest:              {manifest_csv}")
    if collisions_rows:
        print(f"[OK] Wrote collisions:           {collisions_out}")

    for k in sorted(stats.keys()):
        print(f"[INFO] {k}={stats[k]}")


if __name__ == "__main__":
    main()
