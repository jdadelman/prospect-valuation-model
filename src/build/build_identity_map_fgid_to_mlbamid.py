import argparse
import csv
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def strip_accents(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_text(s: str) -> str:
    """
    Lowercase, strip accents, collapse whitespace, drop punctuation-ish separators.
    Intended for stable joins, not display.
    """
    s = strip_accents(s).casefold()
    s = s.strip()
    for ch in ["-", ".", ",", "'", "’", "`", '"', "(", ")", "[", "]", "{", "}", "/", "\\", "|", ":", ";"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())
    return s


def split_first_last(full_name: str) -> tuple[str, str]:
    """
    Conservative:
      - last token = last name
      - remaining tokens = first name
    Known failure modes: compound surnames, suffixes, multi-part last names.
    """
    toks = [t for t in norm_text(full_name).split(" ") if t]
    if not toks:
        return "", ""
    if len(toks) == 1:
        return toks[0], ""
    first = " ".join(toks[:-1])
    last = toks[-1]
    return first, last


def parse_ymd_from_iso(s: str) -> tuple[str, str, str]:
    s = (s or "").strip()
    if not s:
        return "", "", ""
    s = s.split("T", 1)[0]
    parts = s.split("-")
    if len(parts) >= 3:
        return parts[0].strip(), parts[1].strip(), parts[2].strip()
    return "", "", ""


def parse_date_ymd(s: str) -> Optional[date]:
    s = (s or "").strip()
    if not s:
        return None
    s = s.split("T", 1)[0]
    parts = s.split("-")
    if len(parts) < 3:
        return None
    y, m, d = parts[0].strip(), parts[1].strip(), parts[2].strip()
    if not (y.isdigit() and m.isdigit() and d.isdigit()):
        return None
    try:
        return date(int(y), int(m), int(d))
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


@dataclass(frozen=True)
class IdentityRow:
    identity_key: str
    fgid: str
    player_name: str
    player_url: str
    yob: str
    mob: str
    dob: str
    org_abbrevs: str
    published_date_latest: str
    age_float: str
    dob_est_ymd: str
    yob_est: str

@dataclass(frozen=True)
class SpineRow:
    mlbam_id: str
    name_first: str
    name_last: str
    yob: str
    mob: str
    dob: str
    org_abbrevs_seen: str


def read_csv_dicts(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            raise RuntimeError(f"{path}: empty CSV / missing header")
        for row in r:
            yield {k: (row.get(k) or "").strip() for k in (r.fieldnames or [])}


def read_identities(path: Path) -> list[IdentityRow]:
    rows = list(read_csv_dicts(path))
    if not rows:
        raise RuntimeError(f"{path}: parsed zero rows")

    required = {"identity_key", "fgid", "player_name", "player_url"}
    missing = required - set(rows[0].keys())
    if missing:
        raise RuntimeError(f"{path}: missing required columns: {sorted(missing)}")

    out: list[IdentityRow] = []
    for row in rows:
        birth_date = (row.get("birth_date") or row.get("dob_ymd") or "").strip()
        yob, mob, dob = ("", "", "")
        if birth_date:
            yob, mob, dob = parse_ymd_from_iso(birth_date)
        else:
            yob = (row.get("yob") or "").strip()
            mob = (row.get("mob") or "").strip()
            dob = (row.get("dob") or "").strip()

        out.append(
            IdentityRow(
                identity_key=(row.get("identity_key") or "").strip(),
                fgid=(row.get("fgid") or "").strip(),
                player_name=(row.get("player_name") or "").strip(),
                player_url=(row.get("player_url") or "").strip(),
                yob=yob,
                mob=mob,
                dob=dob,
                org_abbrevs=(row.get("org_abbrevs") or "").strip(),
                published_date_latest=(row.get("published_date_latest") or "").strip(),
                age_float=(row.get("age_float") or "").strip(),
                dob_est_ymd=(row.get("dob_est_ymd") or "").strip(),
                yob_est=(row.get("yob_est") or "").strip(),
            )
        )
    
    return out


def read_spine(path: Path) -> list[SpineRow]:
    rows = list(read_csv_dicts(path))
    if not rows:
        raise RuntimeError(f"{path}: parsed zero rows")

    # Detect likely column names (be permissive)
    keys = set(rows[0].keys())

    def pick(*cands: str) -> Optional[str]:
        for c in cands:
            if c in keys:
                return c
        return None

    col_mlbam = pick("mlbam_id")
    col_first = pick("name_first", "first_name", "First", "first")
    col_last = pick("name_last", "last_name", "Last", "last")

    if not col_mlbam or not col_first or not col_last:
        raise RuntimeError(f"{path}: missing required columns for mlbam/name_first/name_last. Found={sorted(keys)}")

    col_birth = pick("birth_date", "DOB", "dob_ymd")
    has_orgs = "org_abbrevs_seen" in keys

    out: list[SpineRow] = []
    for row in rows:
        mlbam = (row.get(col_mlbam) or "").strip()
        if not mlbam:
            continue

        yob, mob, dob = ("", "", "")
        if col_birth and (row.get(col_birth) or "").strip():
            yob, mob, dob = parse_ymd_from_iso((row.get(col_birth) or "").strip())
        else:
            yob = (row.get("yob") or "").strip()
            mob = (row.get("mob") or "").strip()
            dob = (row.get("dob") or "").strip()

        out.append(
            SpineRow(
                mlbam_id=mlbam,
                name_first=(row.get(col_first) or "").strip(),
                name_last=(row.get(col_last) or "").strip(),
                yob=yob,
                mob=mob,
                dob=dob,
                org_abbrevs_seen=(row.get("org_abbrevs_seen") or "").strip() if has_orgs else "",
            )
        )
    if not out:
        raise RuntimeError(f"{path}: parsed zero usable rows")
    return out


def add_to_index(index: dict[tuple[str, ...], list[str]], key: tuple[str, ...], mlbam_id: str) -> None:
    if not mlbam_id:
        return
    index.setdefault(key, []).append(mlbam_id)


def unique_or_none(ids: list[str]) -> Optional[str]:
    if not ids:
        return None
    u = sorted(set(ids))
    if len(u) == 1:
        return u[0]
    return None


def tie_break_by_estimated_dob(
    cand_ids: list[str],
    ident_dob_est_ymd: str,
    spine_dob_by_id: dict[str, Optional[date]],
    tolerance_days: int = 45,
) -> Optional[str]:
    """
    Tie-break by proximity between identity's estimated DOB and MLBAM DOB.
    Only acts if it yields a unique candidate.
    """
    dob_est = parse_date_ymd(ident_dob_est_ymd)
    if dob_est is None:
        return None
    hits: list[str] = []
    for cid in sorted(set(cand_ids)):
        dob_c = spine_dob_by_id.get(cid)
        if dob_c is None:
            continue
        if abs((dob_c - dob_est).days) <= tolerance_days:
            hits.append(cid)
    hits = sorted(set(hits))
    return hits[0] if len(hits) == 1 else None


def main() -> None:
    ap = argparse.ArgumentParser(description="Build FG identity → MLBAM ID crosswalk using MLBAM people spine.")
    ap.add_argument("--identities", default="data/processed/player_identities.csv", help="Input identities CSV.")
    ap.add_argument("--spine", default="data/processed/mlbam_people_spine_2021_2025.csv", help="Input MLBAM spine CSV.")
    ap.add_argument("--out", default="data/intermediate/identity_map_fgid_to_mlbam.csv", help="Output mapping CSV.")
    ap.add_argument(
        "--manifest",
        default="data/intermediate/identity_map_fgid_to_mlbam_manifest.csv",
        help="Output manifest/summary CSV.",
    )
    args = ap.parse_args()

    identities_path = Path(args.identities)
    spine_path = Path(args.spine)
    out_path = Path(args.out)
    manifest_path = Path(args.manifest)

    identities = read_identities(identities_path)
    spine = read_spine(spine_path)

    idx_first_last_dob: dict[tuple[str, str, str, str, str], list[str]] = {}
    idx_last_dob: dict[tuple[str, str, str, str], list[str]] = {}
    idx_full_name: dict[tuple[str], list[str]] = {}

    spine_yob_by_id: dict[str, str] = {}
    spine_orgs_by_id: dict[str, set[str]] = {}
    spine_dob_by_id: dict[str, Optional[date]] = {}

    for s in spine:
        fn = norm_text(s.name_first)
        ln = norm_text(s.name_last)
        full = norm_text(f"{s.name_first} {s.name_last}")

        y, m, d = (s.yob.strip(), s.mob.strip(), s.dob.strip())
        if y and m and d:
            add_to_index(idx_first_last_dob, (fn, ln, y, m, d), s.mlbam_id)
            add_to_index(idx_last_dob, (ln, y, m, d), s.mlbam_id)
            spine_dob_by_id[s.mlbam_id] = parse_date_ymd(f"{y}-{m}-{d}")
        else:
            spine_dob_by_id[s.mlbam_id] = None
        
        if full:
            add_to_index(idx_full_name, (full,), s.mlbam_id)

        spine_yob_by_id[s.mlbam_id] = (s.yob or "").strip()
        if s.org_abbrevs_seen:
            spine_orgs_by_id[s.mlbam_id] = set(x for x in s.org_abbrevs_seen.split("|") if x.strip())

    out_path.parent.mkdir(parents=True, exist_ok=True)

    matched = 0
    ambiguous = 0
    unmatched = 0
    by_status: dict[str, int] = {}

    out_fields = [
        "identity_key",
        "fgid",
        "player_name",
        "player_url",
        "mlbam_id",
        "match_status",
        "match_method",
        "candidate_mlbam_ids",
        "identity_yob",
        "identity_mob",
        "identity_dob",
        "identity_org_abbrevs",
        "identity_published_date_latest",
        "identity_age_float",
        "identity_dob_est_ymd",
        "identity_yob_est",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()

        for ident in identities:
            first_norm, last_norm = split_first_last(ident.player_name)
            full_norm = norm_text(ident.player_name)

            y, m, d = ident.yob.strip(), ident.mob.strip(), ident.dob.strip()
            has_dob = bool(y and m and d)

            mlbam_id: str = ""
            match_method: str = ""
            match_status: str = ""
            candidates: list[str] = []

            # Rule 1: exact normalized first+last + full DOB
            if has_dob:
                candidates = idx_first_last_dob.get((first_norm, last_norm, y, m, d), [])
                u = unique_or_none(candidates)
                if u:
                    mlbam_id = u
                    match_method = "exact_name_dob"
                    match_status = "matched_exact_name_dob"

            # Rule 2: exact last + full DOB
            if not mlbam_id and has_dob:
                candidates = idx_last_dob.get((last_norm, y, m, d), [])
                u = unique_or_none(candidates)
                if u:
                    mlbam_id = u
                    match_method = "lastname_dob"
                    match_status = "matched_lastname_dob"

            # Rule 3: full-name rule (+ tie-breaks if ambiguous)
            if not mlbam_id:
                candidates = idx_full_name.get((full_norm,), [])
                cand_unique = sorted(set(candidates))
                u = unique_or_none(candidates)

                if u:
                    mlbam_id = u
                    match_method = "name_only_unique"
                    match_status = "matched_name_only_unique"
                else:
                    chosen: Optional[str] = None

                    ident_orgs = [x for x in ident.org_abbrevs.split("|") if x.strip()]
                    if ident_orgs and spine_orgs_by_id and cand_unique:
                        hits = []
                        for cid in cand_unique:
                            orgs = spine_orgs_by_id.get(cid, set())
                            if any(o in orgs for o in ident_orgs):
                                hits.append(cid)
                        if len(hits) == 1:
                            chosen = hits[0]
                            match_method = "name_plus_org_unique"
                            match_status = "matched_name_with_tiebreak"

                    if chosen is None and cand_unique:
                        # Prefer estimated DOB proximity (day-level)
                        chosen = tie_break_by_estimated_dob(
                            cand_ids=cand_unique,
                            ident_dob_est_ymd=ident.dob_est_ymd,
                            spine_dob_by_id=spine_dob_by_id,
                            tolerance_days=45,
                        )
                        if chosen is not None:
                            match_method = "name_plus_est_dob_unique"
                            match_status = "matched_name_with_tiebreak"

                    if chosen is None and cand_unique and ident.yob_est and ident.yob_est.isdigit():
                        # Fallback: estimated YOB (year-level, +/-1)
                        est_yob = int(ident.yob_est)
                        hits = []
                        for cid in cand_unique:
                            yob_c = spine_yob_by_id.get(cid, "")
                            if yob_c.isdigit() and abs(int(yob_c) - est_yob) <= 1:
                                hits.append(cid)
                        hits = sorted(set(hits))
                        if len(hits) == 1:
                            chosen = hits[0]
                            match_method = "name_plus_est_yob_unique"
                            match_status = "matched_name_with_tiebreak"

                    if chosen:
                        mlbam_id = chosen

            if mlbam_id:
                matched += 1
                by_status[match_status] = by_status.get(match_status, 0) + 1
            else:
                # classify ambiguity/unmatched primarily from name candidates
                if candidates and len(set(candidates)) > 1:
                    ambiguous += 1
                    match_status = "ambiguous_multiple_candidates"
                    match_method = "name_only"
                    by_status[match_status] = by_status.get(match_status, 0) + 1
                else:
                    unmatched += 1
                    match_status = "unmatched_no_candidate"
                    match_method = "none"
                    by_status[match_status] = by_status.get(match_status, 0) + 1

            w.writerow(
                {
                    "identity_key": ident.identity_key,
                    "fgid": ident.fgid,
                    "player_name": ident.player_name,
                    "player_url": ident.player_url,
                    "mlbam_id": mlbam_id,
                    "match_status": match_status,
                    "match_method": match_method,
                    "candidate_mlbam_ids": "|".join(sorted(set(candidates))) if candidates else "",
                    "identity_yob": y,
                    "identity_mob": m,
                    "identity_dob": d,
                    "identity_org_abbrevs": ident.org_abbrevs,
                    "identity_published_date_latest": ident.published_date_latest,
                    "identity_age_float": ident.age_float,
                    "identity_dob_est_ymd": ident.dob_est_ymd,
                    "identity_yob_est": ident.yob_est,
                }
            )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", newline="", encoding="utf-8") as mf:
        w = csv.DictWriter(mf, fieldnames=["generated_at_utc", "identities", "matched", "ambiguous", "unmatched"])
        w.writeheader()
        w.writerow(
            {
                "generated_at_utc": utc_now_iso(),
                "identities": str(len(identities)),
                "matched": str(matched),
                "ambiguous": str(ambiguous),
                "unmatched": str(unmatched),
            }
        )

        mf.write("\n")
        w2 = csv.DictWriter(mf, fieldnames=["status", "count"])
        w2.writeheader()
        for k in sorted(by_status.keys()):
            w2.writerow({"status": k, "count": str(by_status[k])})

    print(f"[OK] Wrote identity map: {out_path}")
    print(f"[OK] Wrote manifest:    {manifest_path}")
    print(f"[INFO] identities={len(identities)} matched={matched} ambiguous={ambiguous} unmatched={unmatched}")
    for k in sorted(by_status.keys()):
        print(f"[INFO]   {k}: {by_status[k]}")


if __name__ == "__main__":
    main()
