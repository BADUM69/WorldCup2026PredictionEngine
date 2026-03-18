"""
utils/quality_checks.py — All data quality checks for Story 1.4.

Each check returns a dict with:
    name        — human readable check name
    table       — which table it targets
    status      — "pass", "warn", or "fail"
    detail      — what was found
    value       — numeric result (count, pct, etc.)
    threshold   — what we expected
"""
import logging
from utils.db import fetch_one, fetch_all

log = logging.getLogger(__name__)

CHECKS = []


def check(fn):
    """Decorator to register a check function."""
    CHECKS.append(fn)
    return fn


# =============================================================================
# TOURNAMENTS
# =============================================================================

@check
def tournaments_row_count():
    r = fetch_one("SELECT COUNT(*) AS n FROM tournaments")
    n = r["n"]
    # 1930-2022 = 22 editions, plus 2026
    return {
        "name": "Tournament row count",
        "table": "tournaments",
        "value": n,
        "threshold": "22-23",
        "status": "pass" if 22 <= n <= 23 else "warn" if n > 0 else "fail",
        "detail": f"{n} tournament rows found (expected 22 historical + 2026)",
    }


@check
def tournaments_null_host():
    r = fetch_one("SELECT COUNT(*) AS n FROM tournaments WHERE host_country IS NULL")
    n = r["n"]
    return {
        "name": "Tournaments missing host country",
        "table": "tournaments",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "fail",
        "detail": f"{n} tournaments have no host country",
    }


@check
def tournaments_missing_winners():
    r = fetch_one(
        "SELECT COUNT(*) AS n FROM tournaments WHERE year < 2026 AND winner_team_id IS NULL"
    )
    n = r["n"]
    return {
        "name": "Historical tournaments missing winner",
        "table": "tournaments",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "warn",
        "detail": f"{n} completed tournaments have no winner linked",
    }


@check
def tournaments_duplicate_years():
    r = fetch_one(
        "SELECT COUNT(*) AS n FROM (SELECT year FROM tournaments GROUP BY year HAVING COUNT(*) > 1) t"
    )
    n = r["n"]
    return {
        "name": "Duplicate tournament years",
        "table": "tournaments",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "fail",
        "detail": f"{n} duplicate year values found",
    }


# =============================================================================
# TEAMS
# =============================================================================

@check
def teams_row_count():
    r = fetch_one("SELECT COUNT(*) AS n FROM teams")
    n = r["n"]
    # At least 80 nations have appeared across all World Cups
    return {
        "name": "Team row count",
        "table": "teams",
        "value": n,
        "threshold": ">= 80",
        "status": "pass" if n >= 80 else "warn" if n > 0 else "fail",
        "detail": f"{n} teams found (expected 80+ across all tournaments)",
    }


@check
def teams_missing_confederation():
    r = fetch_one("SELECT COUNT(*) AS n FROM teams WHERE confederation IS NULL")
    n = r["n"]
    pct = round(n / max(fetch_one("SELECT COUNT(*) AS t FROM teams")["t"], 1) * 100, 1)
    return {
        "name": "Teams missing confederation",
        "table": "teams",
        "value": pct,
        "threshold": "< 5%",
        "status": "pass" if pct < 5 else "warn" if pct < 20 else "fail",
        "detail": f"{n} teams ({pct}%) have no confederation set",
    }


@check
def teams_missing_fifa_code():
    r = fetch_one("SELECT COUNT(*) AS n FROM teams WHERE fifa_code IS NULL")
    n = r["n"]
    return {
        "name": "Teams missing FIFA code",
        "table": "teams",
        "value": n,
        "threshold": "< 10",
        "status": "pass" if n < 10 else "warn",
        "detail": f"{n} teams have no FIFA 3-letter code",
    }


# =============================================================================
# PLAYERS
# =============================================================================

@check
def players_row_count():
    r = fetch_one("SELECT COUNT(*) AS n FROM players")
    n = r["n"]
    # Rough estimate: ~23 players x ~80 teams x 22 tournaments, minus dupes
    return {
        "name": "Player row count",
        "table": "players",
        "value": n,
        "threshold": ">= 5000",
        "status": "pass" if n >= 5000 else "warn" if n > 0 else "fail",
        "detail": f"{n} players found",
    }


@check
def players_missing_dob():
    total = fetch_one("SELECT COUNT(*) AS t FROM players")["t"]
    r = fetch_one("SELECT COUNT(*) AS n FROM players WHERE date_of_birth IS NULL")
    n = r["n"]
    pct = round(n / max(total, 1) * 100, 1)
    return {
        "name": "Players missing date of birth",
        "table": "players",
        "value": pct,
        "threshold": "< 20%",
        "status": "pass" if pct < 20 else "warn" if pct < 50 else "fail",
        "detail": f"{n} players ({pct}%) have no date of birth — expected for pre-1970 data",
    }


@check
def players_missing_nationality():
    r = fetch_one("SELECT COUNT(*) AS n FROM players WHERE nationality IS NULL")
    n = r["n"]
    return {
        "name": "Players missing nationality",
        "table": "players",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "warn",
        "detail": f"{n} players have no nationality set",
    }


@check
def players_duplicate_names():
    r = fetch_one(
        """
        SELECT COUNT(*) AS n FROM (
            SELECT player_name, nationality
            FROM players
            GROUP BY player_name, nationality
            HAVING COUNT(*) > 1
        ) t
        """
    )
    n = r["n"]
    return {
        "name": "Duplicate player name + nationality combinations",
        "table": "players",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "warn",
        "detail": f"{n} duplicate player+nationality combos — may indicate ingestion duplication",
    }


# =============================================================================
# MATCHES
# =============================================================================

@check
def matches_row_count():
    r = fetch_one("SELECT COUNT(*) AS n FROM matches")
    n = r["n"]
    # 1930-2022: 22 tournaments, ~900 total matches
    return {
        "name": "Match row count",
        "table": "matches",
        "value": n,
        "threshold": ">= 850",
        "status": "pass" if n >= 850 else "warn" if n > 0 else "fail",
        "detail": f"{n} matches found (expected ~900 across 1930-2022)",
    }


@check
def matches_missing_scores():
    r = fetch_one(
        "SELECT COUNT(*) AS n FROM matches WHERE home_score IS NULL OR away_score IS NULL"
    )
    n = r["n"]
    return {
        "name": "Matches missing scores",
        "table": "matches",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "warn",
        "detail": f"{n} matches have no score recorded",
    }


@check
def matches_negative_scores():
    r = fetch_one(
        "SELECT COUNT(*) AS n FROM matches WHERE home_score < 0 OR away_score < 0"
    )
    n = r["n"]
    return {
        "name": "Matches with negative scores",
        "table": "matches",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "fail",
        "detail": f"{n} matches have negative score values — data corruption",
    }


@check
def matches_per_tournament():
    rows = fetch_all(
        """
        SELECT t.year, COUNT(m.match_id) AS match_count
        FROM tournaments t
        LEFT JOIN matches m ON m.tournament_id = t.tournament_id
        WHERE t.year < 2026
        GROUP BY t.year
        ORDER BY t.year
        """
    )
    # Expected counts by era: 1930-1978 ~18-38, 1982-1994 ~52, 1998-2022 ~64
    problems = []
    for row in rows:
        year, count = row["year"], row["match_count"]
        if year <= 1978 and count < 10:
            problems.append(f"{year}: {count} matches")
        elif year >= 1982 and year <= 1994 and count < 40:
            problems.append(f"{year}: {count} matches")
        elif year >= 1998 and count < 55:
            problems.append(f"{year}: {count} matches")

    return {
        "name": "Match count by tournament",
        "table": "matches",
        "value": len(problems),
        "threshold": 0,
        "status": "pass" if not problems else "warn",
        "detail": ("All tournament match counts look correct"
                   if not problems
                   else "Low match counts: " + ", ".join(problems)),
    }


@check
def matches_penalties_without_aet():
    r = fetch_one(
        """
        SELECT COUNT(*) AS n FROM matches
        WHERE went_to_penalties = TRUE AND went_to_extra_time = FALSE
        """
    )
    n = r["n"]
    return {
        "name": "Penalty shootouts without extra time flag",
        "table": "matches",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "fail",
        "detail": f"{n} matches flagged as penalties but not extra time — constraint violation",
    }


# =============================================================================
# MATCH STATS
# =============================================================================

@check
def match_stats_coverage():
    total = fetch_one(
        """
        SELECT COUNT(*) AS n FROM matches m
        JOIN tournaments t ON t.tournament_id = m.tournament_id
        WHERE t.year < 2026
        """
    )["n"]
    covered = fetch_one("SELECT COUNT(DISTINCT match_id) AS n FROM match_stats")["n"]
    pct = round(covered / max(total, 1) * 100, 1)
    return {
        "name": "Match stats coverage",
        "table": "match_stats",
        "value": pct,
        "threshold": "> 60%",
        "status": "pass" if pct > 60 else "warn" if pct > 20 else "fail",
        "detail": (
            f"{covered}/{total} matches have team stats ({pct}%). "
            "Pre-1966 data is expected to be sparse."
        ),
    }


@check
def match_stats_possession_sanity():
    r = fetch_one(
        """
        SELECT COUNT(*) AS n FROM (
            SELECT match_id, SUM(possession_pct) AS total
            FROM match_stats
            WHERE possession_pct IS NOT NULL
            GROUP BY match_id
            HAVING SUM(possession_pct) < 95 OR SUM(possession_pct) > 105
        ) t
        """
    )
    n = r["n"]
    return {
        "name": "Possession percentages sum to ~100 per match",
        "table": "match_stats",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "warn",
        "detail": f"{n} matches where home + away possession does not sum to ~100%",
    }


@check
def match_stats_shots_sanity():
    r = fetch_one(
        "SELECT COUNT(*) AS n FROM match_stats WHERE shots_on_target > shots"
    )
    n = r["n"]
    return {
        "name": "Shots on target never exceeds total shots",
        "table": "match_stats",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "fail",
        "detail": f"{n} rows where shots_on_target > shots — impossible value",
    }


# =============================================================================
# PLAYER MATCH STATS
# =============================================================================

@check
def pms_coverage():
    total = fetch_one(
        "SELECT COUNT(*) AS n FROM matches m JOIN tournaments t ON t.tournament_id = m.tournament_id WHERE t.year < 2026"
    )["n"]
    covered = fetch_one("SELECT COUNT(DISTINCT match_id) AS n FROM player_match_stats")["n"]
    pct = round(covered / max(total, 1) * 100, 1)
    return {
        "name": "Player match stats coverage",
        "table": "player_match_stats",
        "value": pct,
        "threshold": "> 50%",
        "status": "pass" if pct > 50 else "warn" if pct > 20 else "fail",
        "detail": f"{covered}/{total} matches have player-level stats ({pct}%)",
    }


@check
def pms_minutes_sanity():
    r = fetch_one(
        "SELECT COUNT(*) AS n FROM player_match_stats WHERE minutes_played > 120"
    )
    n = r["n"]
    return {
        "name": "Player minutes never exceed 120",
        "table": "player_match_stats",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "fail",
        "detail": f"{n} rows with minutes_played > 120",
    }


@check
def pms_goals_vs_match_goals():
    r = fetch_one(
        """
        SELECT COUNT(*) AS n FROM (
            SELECT m.match_id,
                   m.home_score + m.away_score AS expected,
                   COALESCE(SUM(p.goals), 0)   AS actual
            FROM matches m
            JOIN player_match_stats p ON p.match_id = m.match_id
            WHERE m.home_score IS NOT NULL AND m.away_score IS NOT NULL
            GROUP BY m.match_id, m.home_score, m.away_score
            HAVING ABS((m.home_score + m.away_score) - COALESCE(SUM(p.goals), 0)) > 2
        ) t
        """
    )
    n = r["n"]
    return {
        "name": "Player goals roughly match match scorelines",
        "table": "player_match_stats",
        "value": n,
        "threshold": "< 10",
        "status": "pass" if n < 10 else "warn",
        "detail": (
            f"{n} matches where sum of player goals differs from scoreline by more than 2 "
            "(own goals and data gaps can explain small differences)"
        ),
    }


# =============================================================================
# PLAYER CAREERS
# =============================================================================

@check
def careers_row_count():
    r = fetch_one("SELECT COUNT(*) AS n FROM player_careers")
    n = r["n"]
    return {
        "name": "Player career row count",
        "table": "player_careers",
        "value": n,
        "threshold": ">= 3000",
        "status": "pass" if n >= 3000 else "warn" if n > 0 else "fail",
        "detail": f"{n} player-tournament career rows found",
    }


@check
def careers_orphaned_players():
    r = fetch_one(
        """
        SELECT COUNT(*) AS n FROM player_careers pc
        LEFT JOIN players p ON p.player_id = pc.player_id
        WHERE p.player_id IS NULL
        """
    )
    n = r["n"]
    return {
        "name": "Player career rows with no matching player",
        "table": "player_careers",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "fail",
        "detail": f"{n} career rows point to a non-existent player_id",
    }


# =============================================================================
# TOURNAMENT SQUADS
# =============================================================================

@check
def squads_coverage():
    r = fetch_one(
        """
        SELECT COUNT(DISTINCT tournament_id) AS with_squads FROM tournament_squads
        """
    )
    with_squads = r["with_squads"]
    total = fetch_one("SELECT COUNT(*) AS n FROM tournaments WHERE year <= 2022")["n"]
    pct = round(with_squads / max(total, 1) * 100, 1)
    return {
        "name": "Tournament squad coverage",
        "table": "tournament_squads",
        "value": pct,
        "threshold": "> 50%",
        "status": "pass" if pct > 50 else "warn" if pct > 0 else "fail",
        "detail": f"{with_squads}/{total} tournaments have squad lists ({pct}%)",
    }


@check
def squads_size_sanity():
    r = fetch_one(
        """
        SELECT COUNT(*) AS n FROM (
            SELECT tournament_id, team_id, COUNT(*) AS squad_size
            FROM tournament_squads
            GROUP BY tournament_id, team_id
            HAVING COUNT(*) < 11 OR COUNT(*) > 26
        ) t
        """
    )
    n = r["n"]
    return {
        "name": "Squad sizes between 11 and 26",
        "table": "tournament_squads",
        "value": n,
        "threshold": 0,
        "status": "pass" if n == 0 else "warn",
        "detail": f"{n} team-tournament squads with an unusual number of players (expected 23 or 26)",
    }


# =============================================================================
# SOURCE COVERAGE
# =============================================================================

@check
def source_distribution():
    rows = fetch_all(
        """
        SELECT source, COUNT(*) AS n FROM matches
        WHERE source IS NOT NULL
        GROUP BY source ORDER BY n DESC
        """
    )
    detail = ", ".join(f"{r['source']}: {r['n']}" for r in rows) or "No source tags found"
    has_sources = len(rows) > 0
    return {
        "name": "Match rows by data source",
        "table": "matches",
        "value": len(rows),
        "threshold": "> 0",
        "status": "pass" if has_sources else "warn",
        "detail": detail,
    }


@check
def null_source_rows():
    tables = ["tournaments", "teams", "players", "matches",
              "match_stats", "player_match_stats", "player_careers", "tournament_squads"]
    results = []
    for tbl in tables:
        r = fetch_one(f"SELECT COUNT(*) AS n FROM {tbl} WHERE source IS NULL")
        if r["n"] > 0:
            results.append(f"{tbl}: {r['n']}")
    return {
        "name": "Rows with no source tag",
        "table": "all",
        "value": len(results),
        "threshold": 0,
        "status": "pass" if not results else "warn",
        "detail": (", ".join(results) if results else "All rows have source tags"),
    }
