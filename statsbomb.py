"""
loaders/statsbomb.py — StatsBomb open data loader (2018, 2022 World Cups).

StatsBomb provides free event-level JSON via GitHub. No scraping needed —
we clone/download the repo and parse the JSON files directly.

Setup:
    git clone https://github.com/statsbomb/open-data.git data/statsbomb

Run:
    python -m loaders.statsbomb --year 2022
    python -m loaders.statsbomb --all
"""
import os
import json
import logging
import argparse

from utils.db import (
    bulk_insert, get_or_create_team, get_or_create_player,
    get_tournament_id, fetch_one
)
from utils.clean import (
    normalise_country, normalise_position, safe_int,
    safe_float, clean_str, parse_date
)
from config import DATA_DIR

log = logging.getLogger(__name__)

STATSBOMB_DIR = os.path.join(DATA_DIR, "statsbomb", "data")

# StatsBomb competition IDs for World Cups
WC_COMPETITION_ID = 43
WC_SEASON_IDS = {
    2022: 106,
    2018: 3,
}


def _load_json(path: str) -> dict | list | None:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.error("Could not load %s: %s", path, e)
        return None


def load_tournament(year: int) -> None:
    """Load all StatsBomb match and player data for a World Cup year."""
    if year not in WC_SEASON_IDS:
        log.error("StatsBomb data not available for year %d", year)
        return

    if not os.path.isdir(STATSBOMB_DIR):
        log.error(
            "StatsBomb data directory not found at %s\n"
            "Run: git clone https://github.com/statsbomb/open-data.git data/statsbomb",
            STATSBOMB_DIR
        )
        return

    t_id = get_tournament_id(year)
    if not t_id:
        log.error("Tournament %d not in database — run Kaggle loader first", year)
        return

    season_id  = WC_SEASON_IDS[year]
    matches_path = os.path.join(
        STATSBOMB_DIR, "matches",
        str(WC_COMPETITION_ID), f"{season_id}.json"
    )

    matches = _load_json(matches_path)
    if not matches:
        return

    log.info("StatsBomb: loading %d matches for World Cup %d", len(matches), year)
    for match in matches:
        _load_match(match, t_id)


def _load_match(match: dict, tournament_id: int) -> None:
    """Load a single match's lineups and stats from StatsBomb data."""
    sb_match_id  = match.get("match_id")
    home_name    = normalise_country(match.get("home_team", {}).get("home_team_name", ""))
    away_name    = normalise_country(match.get("away_team", {}).get("away_team_name", ""))
    home_score   = safe_int(match.get("home_score"))
    away_score   = safe_int(match.get("away_score"))

    if not home_name or not away_name:
        return

    home_id = get_or_create_team(home_name)
    away_id = get_or_create_team(away_name)

    # Find the match in our DB
    match_row = fetch_one(
        """
        SELECT match_id FROM matches
        WHERE tournament_id = %s AND home_team_id = %s AND away_team_id = %s
        """,
        (tournament_id, home_id, away_id),
    )

    if not match_row:
        log.warning("Match %s vs %s not found in DB — skipping StatsBomb stats", home_name, away_name)
        return

    match_id = match_row["match_id"]

    # ── Lineups → player_match_stats ────────────────────────────────────────
    lineup_path = os.path.join(
        STATSBOMB_DIR, "lineups", f"{sb_match_id}.json"
    )
    lineups = _load_json(lineup_path)
    if lineups:
        _load_lineups(lineups, match_id, home_id, away_id)

    # ── Events → aggregate per-player stats ─────────────────────────────────
    events_path = os.path.join(
        STATSBOMB_DIR, "events", f"{sb_match_id}.json"
    )
    events = _load_json(events_path)
    if events:
        _load_events(events, match_id)


def _load_lineups(lineups: list, match_id: int, home_id: int, away_id: int) -> None:
    """Parse StatsBomb lineup JSON and upsert into player_match_stats."""
    rows = []
    team_map = {lineups[0]["team_id"]: home_id, lineups[1]["team_id"]: away_id} if len(lineups) >= 2 else {}

    for lineup_team in lineups:
        sb_team_id = lineup_team.get("team_id")
        db_team_id = team_map.get(sb_team_id)
        if not db_team_id:
            continue

        for player in lineup_team.get("lineup", []):
            name     = clean_str(player.get("player_name"))
            pos_raw  = player.get("positions", [{}])[0].get("position", "")
            position = normalise_position(clean_str(pos_raw))
            sb_pos   = player.get("positions", [{}])[0].get("position_id", 0)
            is_start = sb_pos > 0 and sb_pos <= 11

            player_id = get_or_create_player(
                player_name=name,
                position=position,
                source="statsbomb",
            )

            rows.append({
                "match_id":    match_id,
                "player_id":   player_id,
                "team_id":     db_team_id,
                "was_starter": is_start,
                "source":      "statsbomb",
            })

    bulk_insert("player_match_stats", rows, conflict_cols=["match_id", "player_id"])


def _load_events(events: list, match_id: int) -> None:
    """
    Aggregate StatsBomb event data into per-player stats.
    StatsBomb events are very granular — we summarise into our schema's columns.
    """
    from collections import defaultdict
    from utils.db import get_connection

    # Aggregate: player_id → stats dict
    agg = defaultdict(lambda: {
        "goals": 0, "assists": 0, "minutes_played": 0,
        "yellow_cards": 0, "red_cards": 0,
        "shots": 0, "distance_km": 0.0,
    })

    player_id_map = {}  # StatsBomb player_id → our DB player_id

    for event in events:
        sb_pid = event.get("player", {}).get("id")
        if not sb_pid:
            continue

        if sb_pid not in player_id_map:
            name = clean_str(event.get("player", {}).get("name"))
            if name:
                player_id_map[sb_pid] = get_or_create_player(name, source="statsbomb")

        pid = player_id_map.get(sb_pid)
        if not pid:
            continue

        etype = event.get("type", {}).get("name", "")

        if etype == "Shot":
            outcome = event.get("shot", {}).get("outcome", {}).get("name", "")
            if outcome == "Goal":
                agg[pid]["goals"] += 1
            agg[pid]["shots"] += 1

        elif etype == "Pass":
            if event.get("pass", {}).get("goal_assist"):
                agg[pid]["assists"] += 1

        elif etype == "Bad Behaviour":
            card = event.get("bad_behaviour", {}).get("card", {}).get("name", "")
            if "Yellow" in card:
                agg[pid]["yellow_cards"] += 1
            elif "Red" in card:
                agg[pid]["red_cards"] += 1

        # Approximate minutes from timestamp
        minute = safe_int(event.get("minute"), default=0)
        agg[pid]["minutes_played"] = max(agg[pid]["minutes_played"], minute)

    # Upsert into player_match_stats
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for pid, stats in agg.items():
                cur.execute(
                    """
                    UPDATE player_match_stats SET
                        goals         = GREATEST(goals, %s),
                        assists       = GREATEST(assists, %s),
                        minutes_played = GREATEST(COALESCE(minutes_played, 0), %s),
                        yellow_cards  = GREATEST(yellow_cards, %s),
                        red_cards     = GREATEST(red_cards, %s),
                        source        = 'statsbomb'
                    WHERE match_id = %s AND player_id = %s
                    """,
                    (
                        stats["goals"], stats["assists"],
                        stats["minutes_played"],
                        stats["yellow_cards"], stats["red_cards"],
                        match_id, pid,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("  StatsBomb events: aggregated stats for %d players in match %d",
             len(agg), match_id)


def run(years: list[int]) -> None:
    for year in years:
        load_tournament(year)
    log.info("StatsBomb loading complete for years: %s", years)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all:
        run(sorted(WC_SEASON_IDS.keys()))
    elif args.year:
        run([args.year])
    else:
        parser.print_help()
