import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[?#].*$", "", s)          # drop query/fragment
    s = s.rstrip("/")
    s = s.split("/")[-1] or s              # last path segment
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:120] if len(s) > 120 else s


def load_cookie_dict(path: Path) -> dict[str, str]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("cookies file must be a JSON object/dict: {\"name\": \"value\", ...}")
    # only string->string pairs
    out: dict[str, str] = {}
    for k, v in obj.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def looks_like_expected_article(html: str, url: str) -> bool:
    # Low-cost positive checks; intended to reduce silent failures.
    lowered = html.lower()
    slug = slugify(url)
    signals = [
        "fangraphs" in lowered,
        slug in lowered,
        ("rk" in lowered and "fv" in lowered),
    ]
    return any(signals)


def fetch_and_save(
    url: str,
    outdir: Path,
    cookies_json: Optional[Path] = None,
    timeout_sec: int = 30,
) -> tuple[Path, Path]:
    outdir.mkdir(parents=True, exist_ok=True)

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

    if cookies_json is not None:
        session.cookies.update(load_cookie_dict(cookies_json))

    resp = session.get(url, timeout=timeout_sec)
    body = resp.content
    sha256 = hashlib.sha256(body).hexdigest()

    base = slugify(url)
    html_path = outdir / f"{base}.html"
    meta_path = outdir / f"{base}.meta.json"

    html_path.write_bytes(body)

    # Decode for sanity checks (do not modify saved bytes)
    encoding = resp.encoding or "utf-8"
    text = body.decode(encoding, errors="replace")

    meta = {
        "url": url,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "status_code": resp.status_code,
        "content_type": resp.headers.get("Content-Type", ""),
        "encoding": resp.encoding,
        "bytes_len": len(body),
        "sha256": sha256,
        "saved_html": str(html_path.as_posix()),
    }
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    if resp.status_code != 200:
        print(f"[WARN] HTTP {resp.status_code}. Saved response to {html_path}")

    if not looks_like_expected_article(text, url):
        print(
            "[WARN] Page does not match expected article signals. "
            f"Saved to {html_path}. You may have been blocked or received unexpected content."
        )

    return html_path, meta_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch a FanGraphs article page and save raw HTML.")
    ap.add_argument("--url", required=True, help="FanGraphs article URL")
    ap.add_argument(
        "--outdir",
        default="data/raw/fangraphs/org_year_pages",
        help="Directory to save raw HTML",
    )
    ap.add_argument(
        "--cookies-json",
        default=None,
        help="Optional JSON cookie dict: {\"cookie_name\": \"cookie_value\", ...}",
    )
    ap.add_argument("--timeout", type=int, default=30, help="Timeout seconds")
    args = ap.parse_args()

    html_path, meta_path = fetch_and_save(
        url=args.url,
        outdir=Path(args.outdir),
        cookies_json=Path(args.cookies_json) if args.cookies_json else None,
        timeout_sec=args.timeout,
    )

    print("[OK] Saved HTML:", html_path)
    print("     Saved meta:", meta_path)


if __name__ == "__main__":
    main()
