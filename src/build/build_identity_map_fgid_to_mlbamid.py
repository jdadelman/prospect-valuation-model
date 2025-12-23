import argparse
import csv
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from src.utils.text import (
    norm_space as _norm_text_base,
    normalize_person_name_for_match,
    split_first_last_person,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def strip_accents(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_text(s: str) -> str:
    return _norm_text_base(strip_accents(s))


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


@dataclass(frozen=True)
class AncillaryRow:
    mlbam_id: str
    first: str
    last: str
    yob: str
    mob: str
    dob: str


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

        dob_est_ymd = (row.get("dob_est_ymd") or "").strip()
        yob_est = (row.get("yob_est") or "").strip()

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
                dob_est_ymd=dob_est_ymd,
                yob_est=yob_est,
            )
        )

    return out


def read_spine(path: Path) -> list[SpineRow]:
    rows = list(read_csv_dicts(path))
    if not rows:
        raise RuntimeError(f"{path}: parsed zero rows")

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


def read_ancillary(path: Path) -> list[AncillaryRow]:
    rows = list(read_csv_dicts(path))
    if not rows:
        raise RuntimeError(f"{path}: parsed zero rows")

    required = {"MLBAM_ID", "First", "Last", "YOB", "MOB", "DOB"}
    missing = required - set(rows[0].keys())
    if missing:
        raise RuntimeError(f"{path}: missing required columns: {sorted(missing)}")

    out: list[AncillaryRow] = []
    for r in rows:
        mlbam = (r.get("MLBAM_ID") or "").strip()
        if not mlbam:
            continue
        out.append(
            AncillaryRow(
                mlbam_id=mlbam,
                first=(r.get("First") or "").strip(),
                last=(r.get("Last") or "").strip(),
                yob=(r.get("YOB") or "").strip(),
                mob=(r.get("MOB") or "").strip(),
                dob=(r.get("DOB") or "").strip(),
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
    dob_by_id: dict[str, Optional[date]],
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
        dob_c = dob_by_id.get(cid)
        if dob_c is None:
            continue
        if abs((dob_c - dob_est).days) <= tolerance_days:
            hits.append(cid)
    hits = sorted(set(hits))
    return hits[0] if len(hits) == 1 else None


def main() -> None:
    ap = argparse.ArgumentParser(description="Build FG identity → MLBAM ID crosswalk using MLBAM people spine + ancillary.")
    ap.add_argument("--identities", default="data/processed/player_identities.csv", help="Input identities CSV.")
    ap.add_argument("--spine", default="data/processed/mlbam_people_spine_2021_2025.csv", help="Input MLBAM spine CSV.")
    ap.add_argument(
        "--ancillary",
        default="src/resources/MLBAM_ancillary_data.csv",
        help="Optional curated MLBAM ancillary CSV (MLBAM_ID, First, Last, YOB, MOB, DOB).",
    )
    ap.add_argument("--out", default="data/intermediate/identity_map_fgid_to_mlbam.csv", help="Output mapping CSV.")
    ap.add_argument(
        "--manifest",
        default="data/intermediate/identity_map_fgid_to_mlbam_manifest.csv",
        help="Output manifest/summary CSV.",
    )
    ap.add_argument("--debug-name", default="", help="If set, print candidate debug for this exact name (case-insensitive).")
    args = ap.parse_args()

    identities_path = Path(args.identities)
    spine_path = Path(args.spine)
    out_path = Path(args.out)
    manifest_path = Path(args.manifest)

    identities = read_identities(identities_path)
    spine = read_spine(spine_path)

    ancillary_path = Path(args.ancillary)
    ancillary: list[AncillaryRow] = []
    if ancillary_path.exists():
        ancillary = read_ancillary(ancillary_path)

    # Unified indexes (spine + ancillary)
    idx_first_last_dob: dict[tuple[str, str, str, str, str], list[str]] = {}
    idx_last_dob: dict[tuple[str, str, str, str], list[str]] = {}
    idx_full_name: dict[tuple[str], list[str]] = {}
    idx_last_yob: dict[tuple[str, str], list[str]] = {}

    idx_anc_first_last_dob: dict[tuple[str, str, str, str, str], list[str]] = {}
    idx_anc_last_dob: dict[tuple[str, str, str, str], list[str]] = {}
    idx_anc_full_name: dict[tuple[str], list[str]] = {}
    idx_anc_last_yob: dict[tuple[str, str], list[str]] = {}

    # Aux maps for tie-breaks (spine + ancillary for DOB/YOB; orgs only from spine)
    yob_by_id: dict[str, str] = {}
    dob_by_id: dict[str, Optional[date]] = {}
    orgs_by_id: dict[str, set[str]] = {}
    first_initial_by_id: dict[str, str] = {}

    # --- Spine contribution ---
    for s in spine:
        fn = norm_text(s.name_first)
        ln = norm_text(s.name_last)
        full = normalize_person_name_for_match(f"{s.name_first} {s.name_last}")
        if s.mlbam_id not in first_initial_by_id and fn:
            first_initial_by_id[s.mlbam_id] = fn[0]

        y, m, d = (s.yob.strip(), s.mob.strip(), s.dob.strip())
        if fn and ln and y and m and d:
            add_to_index(idx_first_last_dob, (fn, ln, y, m, d), s.mlbam_id)
        if ln and y and m and d:
            add_to_index(idx_last_dob, (ln, y, m, d), s.mlbam_id)
        if ln and y:
            add_to_index(idx_last_yob, (ln, y), s.mlbam_id)

        if full:
            add_to_index(idx_full_name, (full,), s.mlbam_id)

        yob_by_id[s.mlbam_id] = (s.yob or "").strip()
        if y and m and d:
            dob_by_id[s.mlbam_id] = parse_date_ymd(f"{y}-{m}-{d}")
        else:
            dob_by_id[s.mlbam_id] = None

        if s.org_abbrevs_seen:
            orgs_by_id[s.mlbam_id] = set(x for x in s.org_abbrevs_seen.split("|") if x.strip())

    # --- Ancillary contribution (into SAME indexes) ---
    for a in ancillary:
        fn = norm_text(a.first)
        ln = norm_text(a.last)
        full = normalize_person_name_for_match(f"{a.first} {a.last}")
        
        if a.mlbam_id not in first_initial_by_id and fn:
            first_initial_by_id[a.mlbam_id] = fn[0]

        y, m, d = (a.yob.strip(), a.mob.strip(), a.dob.strip())
        if fn and ln and y and m and d:
            add_to_index(idx_anc_first_last_dob, (fn, ln, y, m, d), a.mlbam_id)
        if ln and y and m and d:
            add_to_index(idx_anc_last_dob, (ln, y, m, d), a.mlbam_id)
        if full:
            add_to_index(idx_anc_full_name, (full,), a.mlbam_id)
        if ln and y:
            add_to_index(idx_anc_last_yob, (ln, y), a.mlbam_id)

        # populate yob/dob maps if missing
        if a.mlbam_id not in yob_by_id or not yob_by_id.get(a.mlbam_id, ""):
            yob_by_id[a.mlbam_id] = y
        if a.mlbam_id not in dob_by_id or dob_by_id.get(a.mlbam_id) is None:
            dob_by_id[a.mlbam_id] = parse_date_ymd(f"{y}-{m}-{d}") if (y and m and d) else None

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
        "matched_mlbam_id",  # explicit even when candidates exist
        "identity_yob",
        "identity_mob",
        "identity_dob",
        "identity_org_abbrevs",
        "identity_published_date_latest",
        "identity_age_float",
        "identity_dob_est_ymd",
        "identity_yob_est",
    ]

    debug_target = norm_text(args.debug_name) if args.debug_name else ""

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields)
        w.writeheader()

        for ident in identities:
            first_norm, last_norm = split_first_last_person(ident.player_name)
            full_norm = normalize_person_name_for_match(ident.player_name)

            y, m, d = ident.yob.strip(), ident.mob.strip(), ident.dob.strip()
            has_dob = bool(y and m and d)

            mlbam_id: str = ""
            match_method: str = ""
            match_status: str = ""
            candidates: list[str] = []
            candidates_by_rule: dict[str, list[str]] = {}

            # Rule 1: exact normalized first+last + full DOB (spine+ancillary)
            if has_dob:
                cand = []
                cand.extend(idx_first_last_dob.get((first_norm, last_norm, y, m, d), []))
                cand.extend(idx_anc_first_last_dob.get((first_norm, last_norm, y, m, d), []))
                candidates_by_rule["exact_name_dob"] = cand

                u = unique_or_none(cand)
                if u:
                    mlbam_id = u
                    match_method = "exact_name_dob"
                    match_status = "matched_exact_name_dob"

            # Rule 2: exact last + full DOB (spine+ancillary)
            if not mlbam_id and has_dob:
                cand = []
                cand.extend(idx_last_dob.get((last_norm, y, m, d), []))
                cand.extend(idx_anc_last_dob.get((last_norm, y, m, d), []))
                candidates_by_rule["lastname_dob"] = cand

                u = unique_or_none(cand)
                if u:
                    mlbam_id = u
                    match_method = "lastname_dob"
                    match_status = "matched_lastname_dob"

            # Rule 3: full-name rule (+ tie-breaks if ambiguous) (spine+ancillary)
            if not mlbam_id:
                cand = []
                cand.extend(idx_full_name.get((full_norm,), []))
                cand.extend(idx_anc_full_name.get((full_norm,), []))
                candidates_by_rule["name_only"] = cand

                cand_unique = sorted(set(cand))
                u = unique_or_none(cand)

                if u:
                    mlbam_id = u
                    match_method = "name_only_unique"
                    match_status = "matched_name_only_unique"
                else:
                    chosen: Optional[str] = None

                    # Tie-break A: org abbrev intersection (spine-only org provenance)
                    ident_orgs = [x for x in ident.org_abbrevs.split("|") if x.strip()]
                    if ident_orgs and cand_unique:
                        hits = []
                        for cid in cand_unique:
                            orgs = orgs_by_id.get(cid, set())
                            if any(o in orgs for o in ident_orgs):
                                hits.append(cid)
                        hits = sorted(set(hits))
                        if len(hits) == 1:
                            chosen = hits[0]
                            match_method = "name_plus_org_unique"
                            match_status = "matched_name_with_tiebreak"

                    # Tie-break B: estimated DOB proximity (uses spine+ancillary DOB map)
                    if chosen is None and cand_unique:
                        chosen = tie_break_by_estimated_dob(
                            cand_ids=cand_unique,
                            ident_dob_est_ymd=ident.dob_est_ymd,
                            dob_by_id=dob_by_id,
                            tolerance_days=45,
                        )
                        if chosen is not None:
                            match_method = "name_plus_est_dob_unique"
                            match_status = "matched_name_with_tiebreak"

                    # Tie-break C: estimated YOB +/-1 (uses spine+ancillary YOB map)
                    if chosen is None and cand_unique and ident.yob_est and ident.yob_est.isdigit():
                        est_yob = int(ident.yob_est)
                        hits = []
                        for cid in cand_unique:
                            yob_c = (yob_by_id.get(cid, "") or "").strip()
                            if yob_c.isdigit() and abs(int(yob_c) - est_yob) <= 1:
                                hits.append(cid)
                        hits = sorted(set(hits))
                        if len(hits) == 1:
                            chosen = hits[0]
                            match_method = "name_plus_est_yob_unique"
                            match_status = "matched_name_with_tiebreak"

                    if chosen:
                        mlbam_id = chosen
            
            # Rule 4: last-name-only + estimated YOB (very last resort; only accept unique)
            if not mlbam_id:
                yob_est = (ident.yob_est or "").strip()
                if not (yob_est.isdigit() and len(yob_est) == 4):
                    d_est = parse_date_ymd(ident.dob_est_ymd)
                    if d_est is not None:
                        yob_est = str(d_est.year)

                if yob_est.isdigit():
                    first_norm, last_norm = split_first_last(ident.player_name)
                    first_initial = first_norm[0] if first_norm else ""

                    if last_norm:
                        candidates = []
                        candidates.extend(idx_last_yob.get((last_norm, yob_est), []))
                        if ancillary:
                            candidates.extend(idx_anc_last_yob.get((last_norm, yob_est), []))

                        cand_unique = sorted(set(candidates))

                        # Guard A: org intersection when identity has orgs and candidate has org provenance
                        ident_orgs = [x for x in ident.org_abbrevs.split("|") if x.strip()]
                        if ident_orgs and cand_unique:
                            hits = []
                            for cid in cand_unique:
                                orgs = orgs_by_id.get(cid, set())
                                if orgs and any(o in orgs for o in ident_orgs):
                                    hits.append(cid)
                            hits = sorted(set(hits))
                            if len(hits) == 1:
                                mlbam_id = hits[0]
                                match_method = "lastname_plus_yob_est_plus_org_unique"
                                match_status = "matched_lastname_plus_yob_est_unique"

                        # Guard B: first-initial match
                        if not mlbam_id and first_initial and cand_unique:
                            hits = []
                            for cid in cand_unique:
                                fi = (first_initial_by_id.get(cid, "") or "").strip()
                                if fi and fi == first_initial:
                                    hits.append(cid)
                            hits = sorted(set(hits))
                            if len(hits) == 1:
                                mlbam_id = hits[0]
                                match_method = "lastname_plus_yob_est_plus_first_initial_unique"
                                match_status = "matched_lastname_plus_yob_est_unique"
            
            all_candidates: list[str] = []
            for v in candidates_by_rule.values():
                all_candidates.extend(v)
            all_candidates_unique = sorted(set(all_candidates))

            if mlbam_id:
                matched += 1
                by_status[match_status] = by_status.get(match_status, 0) + 1
            else:
                is_ambiguous = len(all_candidates_unique) > 1
                if is_ambiguous:
                    ambiguous += 1
                    match_status = "ambiguous_multiple_candidates"
                    match_method = "none"
                    by_status[match_status] = by_status.get(match_status, 0) + 1
                else:
                    unmatched += 1
                    match_status = "unmatched_no_candidate"
                    match_method = "none"
                    by_status[match_status] = by_status.get(match_status, 0) + 1

            if debug_target and norm_text(ident.player_name) == debug_target:
                print("[DEBUG]", ident.player_name)
                print("        full_norm:", full_norm)
                print("        candidates:", sorted(set(candidates)))

            w.writerow(
                {
                    "identity_key": ident.identity_key,
                    "fgid": ident.fgid,
                    "player_name": ident.player_name,
                    "player_url": ident.player_url,
                    "mlbam_id": mlbam_id,
                    "match_status": match_status,
                    "match_method": match_method,
                    "candidate_mlbam_ids": "|".join(all_candidates_unique) if all_candidates_unique else "",
                    "matched_mlbam_id": mlbam_id,
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
