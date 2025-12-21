import argparse
import csv
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any


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
    # Accept YYYY-MM-DD prefix even if time exists
    s = s.split("T", 1)[0]
    parts = s.split("-")
    if len(parts) >= 3:
        return parts[0].strip(), parts[1].strip(), parts[2].strip()
    return "", "", ""


def get_first_existing(row: dict[str, str], keys: list[str]) -> str:
    for k in keys:
        v = (row.get(k) or "").strip()
        if v:
            return v
    return ""


@dataclass(frozen=True)
class IdentityRow:
    identity_key: str
    fgid: str
    player_name: str
    player_url: str
    yob: str
    mob: str
    dob: str


@dataclass(frozen=True)
class SpineRow:
    mlbam_id: str
    name_first: str
    name_last: str
    yob: str
    mob: str
    dob: str


def read_identities(path: Path) -> list[IdentityRow]:
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {"identity_key", "fgid", "player_name", "player_url"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise RuntimeError(f"{path}: missing required columns: {sorted(missing)}")

        out: list[IdentityRow] = []
        for row in r:
            # Optional DOB fields
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
                )
            )
        return out


def read_spine(path: Path) -> list[SpineRow]:
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            raise RuntimeError(f"{path}: empty CSV / missing header")

        # Accept a few plausible column variants
        col_mlbam = None
        for cand in ["mlbam_id", "key_mlbam"]:
            if cand in r.fieldnames:
                col_mlbam = cand
                break
        if col_mlbam is None:
            raise RuntimeError(f"{path}: missing MLBAM column (expected mlbam_id or key_mlbam)")

        # name columns
        # (Stats API spine uses name_first/name_last per our script; be permissive)
        col_first = None
        for cand in ["name_first", "first_name", "NameFirst", "First", "first"]:
            if cand in r.fieldnames:
                col_first = cand
                break

        col_last = None
        for cand in ["name_last", "last_name", "NameLast", "Last", "last"]:
            if cand in r.fieldnames:
                col_last = cand
                break

        if col_first is None or col_last is None:
            raise RuntimeError(f"{path}: missing name_first/name_last columns (found={r.fieldnames})")

        out: list[SpineRow] = []
        for row in r:
            mlbam = (row.get(col_mlbam) or "").strip()
            if not mlbam:
                continue

            birth_date = get_first_existing(row, ["birth_date", "DOB", "dob", "dob_ymd"])
            yob, mob, dob = ("", "", "")
            if birth_date:
                yob, mob, dob = parse_ymd_from_iso(birth_date)
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
                )
            )
        if not out:
            raise RuntimeError(f"{path}: parsed zero rows")
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Build FGID→MLBAM identity map from MLBAM people spine.")
    ap.add_argument(
        "--identities",
        default="data/processed/player_identities.csv",
        help="Input identities CSV (Stage A output).",
    )
    ap.add_argument(
        "--spine",
        default="data/processed/mlbam_people_spine_2021_2025.csv",
        help="Input MLBAM people spine CSV.",
    )
    ap.add_argument(
        "--out",
        default="data/intermediate/identity_map_fgid_to_mlbam.csv",
        help="Output mapping CSV.",
    )
    ap.add_argument(
        "--manifest",
        default="data/intermediate/identity_map_fgid_to_mlbam_manifest.csv",
        help="Optional manifest-style summary CSV.",
    )
    args = ap.parse_args()

    identities_path = Path(args.identities)
    spine_path = Path(args.spine)
    out_path = Path(args.out)
    manifest_path = Path(args.manifest)

    identities = read_identities(identities_path)
    spine = read_spine(spine_path)

    # Build indices from spine
    idx_first_last_dob: dict[tuple[str, str, str, str, str], list[str]] = {}
    idx_last_dob: dict[tuple[str, str, str, str], list[str]] = {}
    idx_full_name: dict[tuple[str], list[str]] = {}

    for s in spine:
        fn = norm_text(s.name_first)
        ln = norm_text(s.name_last)
        full = norm_text(f"{s.name_first} {s.name_last}")

        y, m, d = (s.yob.strip(), s.mob.strip(), s.dob.strip())
        # Keep DOB keys only if complete; partial DOB cannot support deterministic rules.
        if y and m and d:
            add_to_index(idx_first_last_dob, (fn, ln, y, m, d), s.mlbam_id)
            add_to_index(idx_last_dob, (ln, y, m, d), s.mlbam_id)

        # Name-only index always
        if full:
            add_to_index(idx_full_name, (full,), s.mlbam_id)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    matched = 0
    ambiguous = 0
    unmatched = 0
    by_method: dict[str, int] = {}

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

            # Rule 1: exact normalized first+last + full DOB (only if DOB present on identity)
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

            # Rule 3: exact full name only if unique
            if not mlbam_id:
                candidates = idx_full_name.get((full_norm,), [])
                u = unique_or_none(candidates)
                if u:
                    mlbam_id = u
                    match_method = "name_only_unique"
                    match_status = "matched_name_only_unique"

            if mlbam_id:
                matched += 1
                by_method[match_status] = by_method.get(match_status, 0) + 1
            else:
                # Determine ambiguous vs unmatched based on name-only candidate multiplicity
                if candidates and len(set(candidates)) > 1:
                    ambiguous += 1
                    match_status = "ambiguous_multiple_candidates"
                    match_method = "name_only"
                    by_method[match_status] = by_method.get(match_status, 0) + 1
                else:
                    unmatched += 1
                    match_status = "unmatched_no_candidate"
                    match_method = "none"
                    by_method[match_status] = by_method.get(match_status, 0) + 1

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
                }
            )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", newline="", encoding="utf-8") as mf:
        w = csv.DictWriter(mf, fieldnames=["generated_at_utc", "identities", "matched", "ambiguous", "unmatched"])
        w.writeheader()
        w.writerow(
            {
                "generated_at_utc": str(utc_now_iso()),
                "identities": str(len(identities)),
                "matched": str(matched),
                "ambiguous": str(ambiguous),
                "unmatched": str(unmatched),
            }
        )

        w2 = csv.DictWriter(mf, fieldnames=["status", "count"])
        mf.write("\n")
        w2.writeheader()
        for k in sorted(by_method.keys()):
            w2.writerow({"status": k, "count": str(by_method[k])})

    print(f"[OK] Wrote identity map: {out_path}")
    print(f"[OK] Wrote manifest:    {manifest_path}")
    print(f"[INFO] identities={len(identities)} matched={matched} ambiguous={ambiguous} unmatched={unmatched}")
    for k in sorted(by_method.keys()):
        print(f"[INFO]   {k}: {by_method[k]}")


if __name__ == "__main__":
    main()
