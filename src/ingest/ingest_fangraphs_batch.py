import argparse
import csv
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests


def slugify_url(url: str) -> str:
    # use last path segment; fallback to hash
    u = url.split("#", 1)[0].split("?", 1)[0].rstrip("/")
    last = u.rsplit("/", 1)[-1] if "/" in u else u
    if not last:
        last = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return last


def load_urls(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    urls: list[str] = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        urls.append(s)
    return urls


def fetch_one(
    session: requests.Session,
    url: str,
    outdir: Path,
    timeout_sec: int,
) -> tuple[Path, Path, dict]:
    outdir.mkdir(parents=True, exist_ok=True)

    resp = session.get(url, timeout=timeout_sec)
    body = resp.content
    sha256 = hashlib.sha256(body).hexdigest()

    slug = slugify_url(url)
    html_path = outdir / f"{slug}.html"
    meta_path = outdir / f"{slug}.meta.json"

    html_path.write_bytes(body)

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

    return html_path, meta_path, meta


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch ingest FanGraphs org-year pages (save raw HTML snapshots).")
    ap.add_argument("--urls", required=True, help="Path to a text file of URLs (one per line)")
    ap.add_argument("--outdir", default="data/raw/fangraphs/org_year_pages", help="Output directory for HTML")
    ap.add_argument("--timeout", type=int, default=30, help="Request timeout seconds")
    ap.add_argument("--sleep", type=float, default=2.0, help="Seconds to sleep between requests")
    ap.add_argument("--manifest", default="data/raw/fangraphs/ingest_manifest.csv", help="Manifest CSV path")
    args = ap.parse_args()

    url_path = Path(args.urls)
    urls = load_urls(url_path)
    if not urls:
        raise SystemExit("No URLs found in urls file.")

    outdir = Path(args.outdir)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

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

    fieldnames = [
        "url",
        "slug",
        "status_code",
        "bytes_len",
        "sha256",
        "saved_html",
        "saved_meta",
        "ok",
        "error",
        "fetched_at_utc",
    ]

    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for i, url in enumerate(urls, start=1):
            slug = slugify_url(url)
            row = {
                "url": url,
                "slug": slug,
                "status_code": "",
                "bytes_len": "",
                "sha256": "",
                "saved_html": "",
                "saved_meta": "",
                "ok": "0",
                "error": "",
                "fetched_at_utc": "",
            }

            try:
                html_path, meta_path, meta = fetch_one(session, url, outdir, args.timeout)
                row["status_code"] = str(meta["status_code"])
                row["bytes_len"] = str(meta["bytes_len"])
                row["sha256"] = meta["sha256"]
                row["saved_html"] = str(html_path.as_posix())
                row["saved_meta"] = str(meta_path.as_posix())
                row["fetched_at_utc"] = meta["fetched_at_utc"]
                row["ok"] = "1" if meta["status_code"] == 200 else "0"
                if meta["status_code"] != 200:
                    row["error"] = f"HTTP {meta['status_code']}"
            except Exception as e:
                row["error"] = str(e)

            w.writerow(row)
            print(f"[{i}/{len(urls)}] {url} -> ok={row['ok']} {row['error']}".strip())

            if i < len(urls):
                time.sleep(args.sleep)

    print(f"[OK] Wrote ingest manifest: {manifest_path}")


if __name__ == "__main__":
    main()
