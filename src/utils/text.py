import re
from typing import Optional


_NON_SLUG_RE = re.compile(r"[^a-z0-9]+")
_MULTI_DASH_RE = re.compile(r"-{2,}")


def norm_space(s: str) -> str:
    return " ".join((s or "").strip().split())


def parse_int(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def slugify(s: str, max_len: int = 120) -> str:
    s = (s or "").strip().lower()
    s = _NON_SLUG_RE.sub("-", s)
    s = _MULTI_DASH_RE.sub("-", s).strip("-")
    return s[:max_len] if len(s) > max_len else s


def sanitize_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("/", " ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s
