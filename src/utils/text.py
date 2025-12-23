import re
from typing import Optional

_NON_SLUG_RE = re.compile(r"[^a-z0-9]+")
_MULTI_DASH_RE = re.compile(r"-{2,}")

# Suffixes to drop only when they appear as the final token.
# Conservative by design to avoid accidental merges.
_SUFFIX_TOKENS = {"jr", "sr", "ii", "iii", "iv", "v"}

# Used for "C.J." / "C J" / "CJ" style initial-first names at the start of a name.
_INITIAL_LEADING_RE = re.compile(r"^\s*([A-Za-z])\s*[.\-]?\s*([A-Za-z])(?:\b|[.\-]|\s)")

_SUFFIXES = {
    "jr", "sr",
    "ii", "iii", "iv", "v", "vi",
}


def normalize_person_name_for_match(s: str) -> str:
    """
    Normalize a human name for matching:
      - lower
      - strip punctuation / separators
      - collapse whitespace
      - drop suffix tokens (jr, sr, ii, iii, ...)
      - normalize middle initials (a., a)
    """
    base = norm_space(s).casefold()

    # Replace punctuation-ish with spaces, but keep letters/numbers
    base = re.sub(r"[^\w\s]", " ", base)  # drops periods in initials, etc.
    base = " ".join(base.split())

    toks = [t for t in base.split(" ") if t]

    # Drop suffix at end (allow multiple, rare but harmless)
    while toks and toks[-1] in _SUFFIXES:
        toks.pop()

    return " ".join(toks)


def split_first_last_person(full_name: str) -> tuple[str, str]:
    """
    Suffix-aware split:
      - normalize_person_name_for_match
      - last token = last name
      - remaining tokens = first name
    """
    s = normalize_person_name_for_match(full_name)
    toks = [t for t in s.split(" ") if t]
    if not toks:
        return "", ""
    if len(toks) == 1:
        return toks[0], ""
    return " ".join(toks[:-1]), toks[-1]


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


def _strip_name_suffix_tokens(s: str) -> str:
    toks = s.split()
    if not toks:
        return s
    last = toks[-1]
    if last in _SUFFIX_TOKENS:
        toks = toks[:-1]
    return " ".join(toks)


def _normalize_leading_initials(s: str) -> str:
    """
    Canonicalize leading "C.J." / "C J" / "CJ" to "cj".
    Applies only at the start of the string and only for exactly two letters.
    """
    m = _INITIAL_LEADING_RE.match(s)
    if not m:
        return s
    a, b = m.group(1), m.group(2)
    # Preserve remainder after matched initials
    rest = s[m.end() :].lstrip()
    return f"{a}{b} {rest}".strip()


def norm_text(s: str) -> str:
    """
    Lowercase, collapse whitespace, drop punctuation-ish separators.
    Intended for stable joins, not display.

    Adds:
      - strip trailing generational suffixes (jr/sr/ii/iii/iv/v)
      - normalize leading initials (C.J. / C J / CJ -> cj)
    """
    s = (s or "").strip().casefold()

    for ch in ["-", ".", ",", "'", "’", "`", '"', "(", ")", "[", "]", "{", "}", "/", "\\", "|", ":", ";"]:
        s = s.replace(ch, " ")
    s = " ".join(s.split())

    s = _normalize_leading_initials(s)
    s = _strip_name_suffix_tokens(s)

    return s


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
