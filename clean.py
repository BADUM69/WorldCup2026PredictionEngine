"""
utils/clean.py — Data cleaning and normalisation helpers.

These functions are called by every loader/scraper before
rows are passed to db.bulk_insert(). The goal is consistent,
schema-ready data regardless of source.
"""
import re
import logging
from datetime import date, datetime

log = logging.getLogger(__name__)

# ── Country name normalisation ──────────────────────────────────────────────
# Sources use different names for the same nation. Map everything to the
# canonical name used in our teams table.
COUNTRY_ALIASES = {
    "west germany":          "Germany",
    "federal republic of germany": "Germany",
    "czech republic":        "Czechia",
    "republic of ireland":   "Ireland",
    "usa":                   "United States",
    "united states of america": "United States",
    "south korea":           "Korea Republic",
    "north korea":           "Korea DPR",
    "ivory coast":           "Côte d'Ivoire",
    "cote d'ivoire":         "Côte d'Ivoire",
    "cape verde":            "Cabo Verde",
    "trinidad & tobago":     "Trinidad and Tobago",
    "trinidad and tobago":   "Trinidad and Tobago",
    "bosnia & herzegovina":  "Bosnia and Herzegovina",
    "chinese taipei":        "Chinese Taipei",
    "england":               "England",          # treated separately from UK
    "soviet union":          "Soviet Union",      # historical — keep as-is
    "yugoslavia":            "Yugoslavia",        # historical
    "czechoslovakia":        "Czechoslovakia",    # historical
}

def normalise_country(name: str) -> str:
    """Return canonical country name."""
    if not name:
        return name
    return COUNTRY_ALIASES.get(name.strip().lower(), name.strip().title())


# ── Score parsing ────────────────────────────────────────────────────────────
def parse_score(score_str: str) -> tuple[int | None, int | None]:
    """
    Parse a score string like '3-1', '2–1', '1 : 2' into (home, away).
    Returns (None, None) if parsing fails.
    """
    if not score_str:
        return None, None
    score_str = score_str.strip().replace("–", "-").replace(" ", "")
    match = re.match(r"^(\d+)[:\-](\d+)", score_str)
    if match:
        return int(match.group(1)), int(match.group(2))
    log.warning("Could not parse score: %r", score_str)
    return None, None


# ── Date parsing ─────────────────────────────────────────────────────────────
DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y",
    "%B %d, %Y", "%d %B %Y", "%b %d, %Y",
    "%Y",
]

def parse_date(date_str: str) -> date | None:
    """Try multiple date formats, return a date object or None."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    log.warning("Could not parse date: %r", date_str)
    return None


# ── Integer cleaning ─────────────────────────────────────────────────────────
def safe_int(value, default=None) -> int | None:
    """Convert to int, returning default if conversion fails."""
    if value is None or str(value).strip() in ("", "-", "N/A", "n/a", "nan"):
        return default
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (ValueError, TypeError):
        log.warning("Could not convert to int: %r", value)
        return default


# ── Float cleaning ───────────────────────────────────────────────────────────
def safe_float(value, default=None) -> float | None:
    """Convert to float, returning default if conversion fails."""
    if value is None or str(value).strip() in ("", "-", "N/A", "n/a", "nan"):
        return default
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        log.warning("Could not convert to float: %r", value)
        return default


# ── String cleaning ──────────────────────────────────────────────────────────
def clean_str(value, max_len: int = None) -> str | None:
    """Strip whitespace, collapse internal spaces, optionally truncate."""
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    if not cleaned or cleaned.lower() in ("n/a", "none", "null", "-"):
        return None
    if max_len and len(cleaned) > max_len:
        log.warning("Truncating string from %d to %d chars", len(cleaned), max_len)
        cleaned = cleaned[:max_len]
    return cleaned


# ── Stage normalisation ──────────────────────────────────────────────────────
STAGE_ALIASES = {
    "group stage":       "Group",
    "group":             "Group",
    "groups":            "Group",
    "round of 16":       "Round of 16",
    "last 16":           "Round of 16",
    "second round":      "Round of 16",
    "r16":               "Round of 16",
    "quarter-final":     "Quarter-final",
    "quarterfinal":      "Quarter-final",
    "qf":                "Quarter-final",
    "semi-final":        "Semi-final",
    "semifinal":         "Semi-final",
    "sf":                "Semi-final",
    "third place":       "Third place",
    "third-place":       "Third place",
    "third place play-off": "Third place",
    "3rd place":         "Third place",
    "final":             "Final",
}

def normalise_stage(stage_str: str) -> str | None:
    """Return a valid match_stage enum value or None."""
    if not stage_str:
        return None
    key = stage_str.strip().lower()
    result = STAGE_ALIASES.get(key)
    if not result:
        log.warning("Unrecognised stage: %r", stage_str)
    return result


# ── Position normalisation ────────────────────────────────────────────────────
POSITION_ALIASES = {
    "goalkeeper":   "GK", "gk":  "GK", "g": "GK",
    "defender":     "DF", "def": "DF", "d": "DF", "cb": "DF", "lb": "DF", "rb": "DF",
    "midfielder":   "MF", "mid": "MF", "m": "MF", "cm": "MF", "dm": "MF", "am": "MF",
    "forward":      "FW", "fwd": "FW", "f": "FW", "st": "FW", "cf": "FW", "lw": "FW",
    "attacker":     "FW", "att": "FW",
    "winger":       "FW",
}

def normalise_position(pos_str: str) -> str | None:
    """Return a valid player_position enum value or None."""
    if not pos_str:
        return None
    return POSITION_ALIASES.get(pos_str.strip().lower())
