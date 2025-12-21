import argparse
import csv
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


# MiLB sportIds: AAA=11, AA=12, High-A=13, Low-A=14, Rookie=16
MILB_SPORT_IDS = [11, 12, 13, 14, 16]
MLB_SPORT_ID = 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_birth_date(birth_date: str) -> tuple[str, str, str]:
    s = (birth_date or "").strip()
    if not s or "-" not in s:
        return "", "", ""
    parts = s.split("-")
    y = parts[0] if len(parts) > 0 else ""
    m = parts[1] if len(parts) > 1 else ""
    d = parts[2] if len(parts) > 2 else ""
    return y, m, d


def backoff_sleep(attempt: int, base: float, cap: float) -> None:
    wait = min(cap, base * (2 ** attempt))
    wait *= random.uniform(0.5, 1.5)
    time.sleep(wait)


def request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout_sec: int,
    retries: int,
    backoff_base: float,
    backoff_cap: float,
) -> dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, params=params, timeout=(min(10, timeout_sec), timeout_sec))
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            if attempt < retries:
                backoff_sleep(attempt, backoff_base, backoff_cap)
                continue
            raise RuntimeError(f"Request failed: url={url} params={params} err={e}") from e
    raise RuntimeError(f"Request failed: {last_err}")


def maybe_cache_write(path: Path, obj: Any, enabled: bool) -> None:
    if not enabled:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def get_mlb_parentorg_abbrev_map(
    session: requests.Session,
    season: int,
    timeout_sec: int,
    retries: int,
    backoff_base: float,
    backoff_cap: float,
) -> dict[int, str]:
    url = "https://statsapi.mlb.com/api/v1/teams"
    data = request_json(
        session=session,
        url=url,
        params={"season": season, "sportIds": str(MLB_SPORT_ID)},
        timeout_sec=timeout_sec,
        retries=retries,
        backoff_base=backoff_base,
        backoff_cap=backoff_cap,
    )
    teams = data.get("teams", [])
    teams = teams if isinstance(teams, list) else []

    out: dict[int, str] = {}
    for t in teams:
        tid = t.get("id")
        ab = t.get("abbreviation")
        if isinstance(tid, int) and isinstance(ab, str) and ab.strip():
            out[tid] = ab.strip()
    return out


def get_milb_teams_and_parent_abbrev(
    session: requests.Session,
    season: int,
    timeout_sec: int,
    retries: int,
    backoff_base: float,
    backoff_cap: float,
    cache_dir: Path,
    cache_enabled: bool,
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    """
    Returns:
      - list of MiLB teams
      - mapping team_id -> parent MLB org abbreviation (e.g., 'ATL')
    """
    url = "https://statsapi.mlb.com/api/v1/teams"
    params = {"season": season, "sportIds": ",".join(str(x) for x in MILB_SPORT_IDS)}
    data = request_json(
        session=session,
        url=url,
        params=params,
        timeout_sec=timeout_sec,
        retries=retries,
        backoff_base=backoff_base,
        backoff_cap=backoff_cap,
    )
    maybe_cache_write(cache_dir / "teams" / f"teams_milb_{season}.json", data, cache_enabled)
    teams = data.get("teams", [])
    teams = teams if isinstance(teams, list) else []

    parent_id_to_abbrev = get_mlb_parentorg_abbrev_map(
        session=session,
        season=season,
        timeout_sec=timeout_sec,
        retries=retries,
        backoff_base=backoff_base,
        backoff_cap=backoff_cap,
    )

    team_id_to_parent_abbrev: dict[int, str] = {}
    for t in teams:
        tid = t.get("id")
        parent = t.get("parentOrgId")
        if isinstance(tid, int) and isinstance(parent, int):
            ab = parent_id_to_abbrev.get(parent, "")
            if ab:
                team_id_to_parent_abbrev[tid] = ab
    return teams, team_id_to_parent_abbrev


@dataclass(frozen=True)
class RosterTask:
    season: int
    team_id: int


def fetch_roster_ids(
    session: requests.Session,
    task: RosterTask,
    timeout_sec: int,
    retries: int,
    backoff_base: float,
    backoff_cap: float,
) -> tuple[RosterTask, list[int], Optional[str]]:
    url = f"https://statsapi.mlb.com/api/v1/teams/{task.team_id}/roster"
    params = {"season": task.season}

    try:
        data = request_json(
            session=session,
            url=url,
            params=params,
            timeout_sec=timeout_sec,
            retries=retries,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
        )
        roster = data.get("roster", [])
        ids: list[int] = []
        if isinstance(roster, list):
            for p in roster:
                try:
                    ids.append(int(p["person"]["id"]))
                except Exception:
                    continue
        return task, ids, None
    except Exception as e:
        return task, [], f"{type(e).__name__}: {e}"


def fetch_people_chunk(
    session: requests.Session,
    person_ids: list[int],
    timeout_sec: int,
    retries: int,
    backoff_base: float,
    backoff_cap: float,
) -> tuple[list[int], list[dict[str, Any]], Optional[str]]:
    if not person_ids:
        return person_ids, [], None
    url = "https://statsapi.mlb.com/api/v1/people"
    params = {"personIds": ",".join(str(x) for x in person_ids)}

    try:
        data = request_json(
            session=session,
            url=url,
            params=params,
            timeout_sec=timeout_sec,
            retries=retries,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
        )
        people = data.get("people", [])
        return person_ids, (people if isinstance(people, list) else []), None
    except Exception as e:
        return person_ids, [], f"{type(e).__name__}: {e}"


def extract_code(field: Any, key: str) -> str:
    """
    StatsAPI typically uses dict fields like {"code":"R"}, but sometimes may produce a string.
    This function is defensive: returns "" if not parseable.
    """
    if isinstance(field, dict):
        v = field.get(key, "")
        return "" if v is None else str(v)
    if isinstance(field, str):
        return field.strip()
    return ""


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Build MLBAM people spine from MiLB team rosters (sportIds 11/12/13/14/16) for seasons 2021–2025. "
            "Outputs MLBAM ID + name + DOB + bats/throws + height/weight + position + org abbrevs seen."
        )
    )
    ap.add_argument("--start-year", type=int, default=2021)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--out", default="data/processed/mlbam_people_spine_2021_2025.csv")

    # Performance controls
    ap.add_argument("--roster-workers", type=int, default=24, help="Concurrent roster requests")
    ap.add_argument("--people-workers", type=int, default=16, help="Concurrent people requests")
    ap.add_argument("--people-chunk", type=int, default=500, help="People endpoint chunk size")

    # Reliability controls
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--backoff-base", type=float, default=0.5)
    ap.add_argument("--backoff-cap", type=float, default=8.0)

    # Optional caching (off by default for speed)
    ap.add_argument("--cache-dir", default="data/raw/statsapi_milb_spine_fast")
    ap.add_argument("--cache", action="store_true", help="Enable caching of /teams responses (small)")

    # Optional manifest
    ap.add_argument("--manifest", default="data/raw/statsapi_milb_spine_fast/manifest.csv")

    args = ap.parse_args()

    start_year = int(args.start_year)
    end_year = int(args.end_year)
    if start_year > end_year:
        raise SystemExit("start-year must be <= end-year")

    out_path = Path(args.out)
    cache_dir = Path(args.cache_dir)
    manifest_path = Path(args.manifest)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
    )

    roster_tasks: list[RosterTask] = []
    team_counts_by_year: dict[int, int] = {}
    team_parent_abbrev_by_season: dict[int, dict[int, str]] = {}

    for season in range(start_year, end_year + 1):
        teams, team_id_to_parent_abbrev = get_milb_teams_and_parent_abbrev(
            session=session,
            season=season,
            timeout_sec=args.timeout,
            retries=args.retries,
            backoff_base=args.backoff_base,
            backoff_cap=args.backoff_cap,
            cache_dir=cache_dir,
            cache_enabled=bool(args.cache),
        )
        team_parent_abbrev_by_season[season] = team_id_to_parent_abbrev

        team_ids = []
        for t in teams:
            tid = t.get("id")
            if isinstance(tid, int):
                team_ids.append(tid)
        team_ids = sorted(set(team_ids))
        team_counts_by_year[season] = len(team_ids)

        for tid in team_ids:
            roster_tasks.append(RosterTask(season=season, team_id=tid))

    if not roster_tasks:
        raise SystemExit("No roster tasks created (no teams found).")

    seasons_seen: dict[int, set[int]] = {}
    org_abbrevs_seen: dict[int, set[str]] = {}
    roster_errors = 0

    roster_manifest_rows: list[dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=max(1, int(args.roster_workers))) as ex:
        futures = [
            ex.submit(
                fetch_roster_ids,
                session,
                task,
                int(args.timeout),
                int(args.retries),
                float(args.backoff_base),
                float(args.backoff_cap),
            )
            for task in roster_tasks
        ]

        done = 0
        total = len(futures)

        for fut in as_completed(futures):
            task, ids, err = fut.result()
            done += 1

            roster_manifest_rows.append(
                {
                    "kind": "roster",
                    "season": str(task.season),
                    "team_id": str(task.team_id),
                    "count": str(len(ids)),
                    "ok": "1" if err is None else "0",
                    "error": "" if err is None else err,
                }
            )

            if err is not None:
                roster_errors += 1
            else:
                parent_map = team_parent_abbrev_by_season.get(task.season, {})
                team_abbrev = parent_map.get(task.team_id, "")

                for pid in ids:
                    seasons_seen.setdefault(pid, set()).add(task.season)
                    if team_abbrev:
                        org_abbrevs_seen.setdefault(pid, set()).add(team_abbrev)

            if done % 200 == 0 or done == total:
                print(f"[ROSTERS] {done}/{total} done; players so far={len(seasons_seen)}; errors={roster_errors}")

    all_player_ids = sorted(seasons_seen.keys())
    if not all_player_ids:
        raise SystemExit("No player IDs collected from rosters.")

    people_chunk = max(1, int(args.people_chunk))
    chunks: list[list[int]] = [all_player_ids[i : i + people_chunk] for i in range(0, len(all_player_ids), people_chunk)]

    bios: dict[int, dict[str, str]] = {}
    people_errors = 0
    people_manifest_rows: list[dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=max(1, int(args.people_workers))) as ex:
        futures = [
            ex.submit(
                fetch_people_chunk,
                session,
                chunk_ids,
                int(args.timeout),
                int(args.retries),
                float(args.backoff_base),
                float(args.backoff_cap),
            )
            for chunk_ids in chunks
        ]

        done = 0
        total = len(futures)

        for fut in as_completed(futures):
            ids_chunk, people, err = fut.result()
            done += 1

            people_manifest_rows.append(
                {
                    "kind": "people",
                    "season": "",
                    "team_id": "",
                    "count": str(len(ids_chunk)),
                    "ok": "1" if err is None else "0",
                    "error": "" if err is None else err,
                }
            )

            if err is not None:
                people_errors += 1
            else:
                for p in people:
                    try:
                        pid = int(p.get("id"))
                    except Exception:
                        continue

                    birth = str(p.get("birthDate", "") or "")
                    yob, mob, dob_day = parse_birth_date(birth)

                    bat_side = p.get("batSide")
                    pitch_hand = p.get("pitchHand")
                    primary_pos = p.get("primaryPosition")

                    bats = extract_code(bat_side, "code")
                    throws = extract_code(pitch_hand, "code")

                    pos = ""
                    if isinstance(primary_pos, dict):
                        v = primary_pos.get("abbreviation", "")
                        pos = "" if v is None else str(v)
                    elif isinstance(primary_pos, str):
                        pos = primary_pos.strip()

                    name_full = str(p.get("fullName", "") or "")
                    birth_date = birth

                    # Only store bios if at least one key identifier exists
                    if name_full or birth_date:
                        bios[pid] = {
                            "name_full": name_full,
                            "name_first": str(p.get("firstName", "") or ""),
                            "name_last": str(p.get("lastName", "") or ""),
                            "birth_date": birth_date,
                            "yob": yob,
                            "mob": mob,
                            "dob": dob_day,
                            "bats": bats,
                            "throws": throws,
                            "pos": pos,
                            "height": str(p.get("height", "") or ""),
                            "weight": str(p.get("weight", "") or ""),
                        }

            if done % 50 == 0 or done == total:
                print(f"[PEOPLE] {done}/{total} chunks done; bios={len(bios)}; errors={people_errors}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "mlbam_id",
        "name_full",
        "name_first",
        "name_last",
        "birth_date",
        "yob",
        "mob",
        "dob",
        "bats",
        "throws",
        "pos",
        "height",
        "weight",
        "seasons_seen",
        "org_abbrevs_seen",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for pid in all_player_ids:
            b = bios.get(pid, {})
            seasons = sorted(seasons_seen.get(pid, set()))
            orgs = sorted(org_abbrevs_seen.get(pid, set()))
            w.writerow(
                {
                    "mlbam_id": str(pid),
                    "name_full": b.get("name_full", ""),
                    "name_first": b.get("name_first", ""),
                    "name_last": b.get("name_last", ""),
                    "birth_date": b.get("birth_date", ""),
                    "yob": b.get("yob", ""),
                    "mob": b.get("mob", ""),
                    "dob": b.get("dob", ""),
                    "bats": b.get("bats", ""),
                    "throws": b.get("throws", ""),
                    "pos": b.get("pos", ""),
                    "height": b.get("height", ""),
                    "weight": b.get("weight", ""),
                    "seasons_seen": ",".join(str(x) for x in seasons),
                    "org_abbrevs_seen": "|".join(orgs),
                }
            )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", newline="", encoding="utf-8") as mf:
        w = csv.DictWriter(mf, fieldnames=["kind", "season", "team_id", "count", "ok", "error"])
        w.writeheader()
        for r in roster_manifest_rows:
            w.writerow(r)
        for r in people_manifest_rows:
            w.writerow(r)

    print(f"[OK] Wrote people spine: {out_path}")
    print(f"[OK] Wrote manifest:    {manifest_path}")
    print(f"[INFO] Teams per season: {', '.join(f'{y}:{team_counts_by_year[y]}' for y in sorted(team_counts_by_year))}")
    print(f"[INFO] Players: {len(all_player_ids)} | roster_errors={roster_errors} | people_chunk_errors={people_errors}")


if __name__ == "__main__":
    main()
