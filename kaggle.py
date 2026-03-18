"""
loaders/kaggle.py — Phase 1 bootstrap loader.

Downloads and ingests pre-cleaned Kaggle World Cup CSVs into PostgreSQL.
This is the fastest path to a working database — no scraping required.

Kaggle datasets used (download manually and place in data/kaggle/):
  - FIFA World Cup (Kaggle: martj42/international-football-results-from-1872-to-2017)
  - World Cup matches (Kaggle: abecklas/fifa-world-cup)

Run:
    python -m loaders.kaggle

Expected files in data/kaggle/:
    WorldCupMatches.csv
    WorldCups.csv
    WorldCupPlayers.csv
"""
import os
import logging
import pandas as pd
from utils.db import (
    get_connection, bulk_insert,
    get_or_create_team, get_tournament_id
)
from utils.clean import (
    normalise_country, normalise_stage, normalise_position,
    safe_int, safe_float, parse_date, clean_str, parse_score
)
from config import KAGGLE_DIR

log = logging.getLogger(__name__)


def load_tournaments(df: pd.DataFrame) -> dict[str, int]:
    """
    Load WorldCups.csv into the tournaments table.
    Returns a dict of {year: tournament_id} for use by subsequent loaders.
    """
    log.info("Loading tournaments...")
    rows = []
    for _, row in df.iterrows():
        year = safe_int(row.get("Year"))
        if not year:
            continue
        rows.append({
            "year":         year,
            "host_country": normalise_country(clean_str(row.get("Country"))),
            "num_teams":    safe_int(row.get("QualifiedTeams"), default=32),
            "total_goals":  safe_int(row.get("GoalsScored")),
            "total_matches":safe_int(row.get("MatchesPlayed")),
            "source":       "kaggle",
        })

    bulk_insert("tournaments", rows, conflict_cols=["year"])
    log.info("Tournaments loaded: %d rows", len(rows))

    # Return year → tournament_id mapping
    from utils.db import fetch_all
    return {r["year"]: r["tournament_id"]
            for r in fetch_all("SELECT tournament_id, year FROM tournaments")}


def load_matches(df: pd.DataFrame, tournament_map: dict) -> None:
    """Load WorldCupMatches.csv into the matches table."""
    log.info("Loading matches...")
    rows = []
    for _, row in df.iterrows():
        year = safe_int(row.get("Year"))
        t_id = tournament_map.get(year)
        if not t_id:
            log.warning("No tournament found for year %s — skipping match", year)
            continue

        home_country = normalise_country(clean_str(row.get("Home Team Name")))
        away_country = normalise_country(clean_str(row.get("Away Team Name")))
        if not home_country or not away_country:
            continue

        home_id = get_or_create_team(home_country)
        away_id = get_or_create_team(away_country)

        stage = normalise_stage(clean_str(row.get("Stage")))

        rows.append({
            "tournament_id":     t_id,
            "home_team_id":      home_id,
            "away_team_id":      away_id,
            "stage":             stage or "Group",
            "match_date":        parse_date(str(row.get("Datetime", ""))),
            "venue":             clean_str(row.get("Stadium"), max_len=150),
            "city":              clean_str(row.get("City"), max_len=100),
            "attendance":        safe_int(row.get("Attendance")),
            "home_score":        safe_int(row.get("Home Team Goals")),
            "away_score":        safe_int(row.get("Away Team Goals")),
            "source":            "kaggle",
        })

    bulk_insert("matches", rows,
                conflict_cols=["tournament_id", "home_team_id", "away_team_id", "match_date"])
    log.info("Matches loaded: %d rows", len(rows))


def load_players(df: pd.DataFrame, tournament_map: dict) -> None:
    """Load WorldCupPlayers.csv into player_match_stats and player_careers."""
    log.info("Loading players from Kaggle...")
    from utils.db import get_or_create_player, fetch_one

    pms_rows     = []
    career_agg   = {}  # (player_id, tournament_id) → aggregated stats

    for _, row in df.iterrows():
        # Safely extract year — don't use `or` which treats 0 as falsy
        raw_year = row.get("Year")
        if raw_year is not None and str(raw_year).strip().isdigit():
            year = int(raw_year)
        elif row.get("RoundID"):
            year = safe_int(str(row.get("RoundID"))[:4])
        else:
            year = None
        if not year:
            continue
        t_id = tournament_map.get(year)
        if not t_id:
            continue

        player_name = clean_str(row.get("Player Name") or row.get("Coach Name"))
        nationality = normalise_country(clean_str(row.get("Team Initials") or row.get("Nationality")))
        if not player_name:
            continue

        position_raw = clean_str(row.get("Position"))
        position = normalise_position(position_raw) if position_raw else None

        player_id = get_or_create_player(
            player_name=player_name,
            nationality=nationality,
            position=position,
            source="kaggle",
        )

        team_id = get_or_create_team(nationality) if nationality else None
        if not team_id:
            continue

        # Look up match
        match_row = fetch_one(
            """
            SELECT match_id FROM matches
            WHERE tournament_id = %s
              AND (home_team_id = %s OR away_team_id = %s)
            LIMIT 1
            """,
            (t_id, team_id, team_id),
        )
        if not match_row:
            continue

        match_id = match_row["match_id"]
        goals    = safe_int(row.get("Goals Scored"), default=0)

        pms_rows.append({
            "match_id":      match_id,
            "player_id":     player_id,
            "team_id":       team_id,
            "goals":         goals,
            "was_starter":   True,   # Kaggle doesn't distinguish starters vs subs — FBref fills this in
            "source":        "kaggle",
        })

        # Accumulate for player_careers
        key = (player_id, t_id, team_id)
        if key not in career_agg:
            career_agg[key] = {"appearances": 0, "goals": 0}
        career_agg[key]["appearances"] += 1
        career_agg[key]["goals"]       += goals or 0

    bulk_insert("player_match_stats", pms_rows,
                conflict_cols=["match_id", "player_id"])

    career_rows = [
        {
            "player_id":    k[0],
            "tournament_id": k[1],
            "team_id":      k[2],
            "appearances":  v["appearances"],
            "goals":        v["goals"],
            "source":       "kaggle",
        }
        for k, v in career_agg.items()
    ]
    bulk_insert("player_careers", career_rows,
                conflict_cols=["player_id", "tournament_id"])
    log.info("Players loaded: %d match-stat rows, %d career rows",
             len(pms_rows), len(career_rows))


def run() -> None:
    """Entry point — load all Kaggle CSVs in order."""
    os.makedirs(KAGGLE_DIR, exist_ok=True)

    wc_path      = os.path.join(KAGGLE_DIR, "WorldCups.csv")
    matches_path = os.path.join(KAGGLE_DIR, "WorldCupMatches.csv")
    players_path = os.path.join(KAGGLE_DIR, "WorldCupPlayers.csv")

    missing = [p for p in [wc_path, matches_path, players_path]
               if not os.path.exists(p)]
    if missing:
        log.error(
            "Missing Kaggle CSV files. Download from Kaggle and place in %s:\n  %s",
            KAGGLE_DIR, "\n  ".join(missing),
        )
        return

    wc_df      = pd.read_csv(wc_path,      encoding="utf-8")
    matches_df = pd.read_csv(matches_path, encoding="utf-8")
    players_df = pd.read_csv(players_path, encoding="utf-8")

    log.info("Kaggle CSVs loaded — %d tournaments, %d matches, %d player rows",
             len(wc_df), len(matches_df), len(players_df))

    tournament_map = load_tournaments(wc_df)
    load_matches(matches_df, tournament_map)
    load_players(players_df, tournament_map)

    log.info("Kaggle ingestion complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    run()
