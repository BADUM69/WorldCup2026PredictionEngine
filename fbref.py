"""
scrapers/fbref.py — FBref match and player statistics scraper.

FBref is the primary source for match_stats and player_match_stats.
Robots.txt permits scraping with respectful delays — we enforce
SCRAPE_DELAY_SECONDS between every request.

Run:
    python -m scrapers.fbref --year 2022
    python -m scrapers.fbref --all       # scrapes all tournaments
"""
import time
import logging
import argparse
import requests
from bs4 import BeautifulSoup

from utils.db import (
    bulk_insert, get_or_create_team, get_or_create_player,
    get_tournament_id, fetch_one
)
from utils.clean import (
    normalise_country, normalise_stage, normalise_position,
    safe_int, safe_float, clean_str, parse_date
)
from config import SCRAPE_DELAY_SECONDS, REQUEST_TIMEOUT, MAX_RETRIES, USER_AGENT

log = logging.getLogger(__name__)

BASE_URL = "https://fbref.com"

# FBref World Cup season URLs by year
TOURNAMENT_URLS = {
    2022: "/en/comps/1/2022/2022-FIFA-World-Cup-Stats",
    2018: "/en/comps/1/2018/2018-FIFA-World-Cup-Stats",
    2014: "/en/comps/1/2014/2014-FIFA-World-Cup-Stats",
    2010: "/en/comps/1/2010/2010-FIFA-World-Cup-Stats",
    2006: "/en/comps/1/2006/2006-FIFA-World-Cup-Stats",
    2002: "/en/comps/1/2002/2002-FIFA-World-Cup-Stats",
    1998: "/en/comps/1/1998/1998-FIFA-World-Cup-Stats",
    1994: "/en/comps/1/1994/1994-FIFA-World-Cup-Stats",
    1990: "/en/comps/1/1990/1990-FIFA-World-Cup-Stats",
    1986: "/en/comps/1/1986/1986-FIFA-World-Cup-Stats",
    1982: "/en/comps/1/1982/1982-FIFA-World-Cup-Stats",
    1978: "/en/comps/1/1978/1978-FIFA-World-Cup-Stats",
    1974: "/en/comps/1/1974/1974-FIFA-World-Cup-Stats",
    1970: "/en/comps/1/1970/1970-FIFA-World-Cup-Stats",
    1966: "/en/comps/1/1966/1966-FIFA-World-Cup-Stats",
}

session = requests.Session()
session.headers["User-Agent"] = USER_AGENT


def _get(url: str) -> BeautifulSoup | None:
    """
    Fetch a URL with retries and rate limiting.
    Returns a BeautifulSoup object or None on failure.
    """
    full_url = BASE_URL + url if url.startswith("/") else url
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(SCRAPE_DELAY_SECONDS)
            resp = session.get(full_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, full_url, e)
            if attempt == MAX_RETRIES:
                log.error("Giving up on %s after %d attempts", full_url, MAX_RETRIES)
                return None
            time.sleep(SCRAPE_DELAY_SECONDS * attempt)  # back off on retry


def scrape_tournament(year: int) -> None:
    """Scrape all match and player stats for a given World Cup year."""
    if year not in TOURNAMENT_URLS:
        log.error("No FBref URL configured for year %d", year)
        return

    t_id = get_tournament_id(year)
    if not t_id:
        log.error("Tournament %d not in database — run Kaggle loader first", year)
        return

    log.info("Scraping FBref for World Cup %d...", year)
    soup = _get(TOURNAMENT_URLS[year])
    if not soup:
        return

    # Find all match report links from the fixtures/results table
    match_links = []
    for a in soup.select("td.left a[href*='/matches/']"):
        href = a.get("href", "")
        if href and href not in match_links:
            match_links.append(href)

    log.info("Found %d match links for %d", len(match_links), year)

    for href in match_links:
        scrape_match(href, t_id)


def scrape_match(match_url: str, tournament_id: int) -> None:
    """Scrape a single match report page for stats."""
    soup = _get(match_url)
    if not soup:
        return

    # ── Scorebox: teams and score ───────────────────────────────────────────
    scorebox = soup.select_one("div.scorebox")
    if not scorebox:
        log.warning("No scorebox found at %s", match_url)
        return

    team_divs = scorebox.select("div[itemprop='performer']")
    if len(team_divs) < 2:
        return

    home_name = normalise_country(clean_str(team_divs[0].get_text()))
    away_name = normalise_country(clean_str(team_divs[1].get_text()))

    score_els = scorebox.select("div.score")
    home_score = safe_int(score_els[0].get_text()) if len(score_els) > 0 else None
    away_score = safe_int(score_els[1].get_text()) if len(score_els) > 1 else None

    home_id = get_or_create_team(home_name, source="fbref") if home_name else None
    away_id = get_or_create_team(away_name, source="fbref") if away_name else None

    if not home_id or not away_id:
        log.warning("Could not resolve teams at %s", match_url)
        return

    # Look up match in DB (already loaded by Kaggle) or insert if missing
    match_row = fetch_one(
        """
        SELECT match_id FROM matches
        WHERE tournament_id = %s AND home_team_id = %s AND away_team_id = %s
        """,
        (tournament_id, home_id, away_id),
    )

    if not match_row:
        # Insert the match if Kaggle didn't have it
        from utils.db import execute
        from utils.clean import normalise_stage
        meta = soup.select_one("div.scorebox_meta")
        stage_text = clean_str(meta.get_text()) if meta else None
        stage = normalise_stage(stage_text) if stage_text else "Group"
        execute(
            """
            INSERT INTO matches (tournament_id, home_team_id, away_team_id,
                                 stage, home_score, away_score, source)
            VALUES (%s, %s, %s, %s, %s, %s, 'fbref')
            ON CONFLICT DO NOTHING
            """,
            (tournament_id, home_id, away_id, stage, home_score, away_score),
        )
        match_row = fetch_one(
            "SELECT match_id FROM matches WHERE tournament_id=%s AND home_team_id=%s AND away_team_id=%s",
            (tournament_id, home_id, away_id),
        )
        if not match_row:
            return

    match_id = match_row["match_id"]

    # ── Team stats tables ───────────────────────────────────────────────────
    for team_id, side in [(home_id, "home"), (away_id, "away")]:
        _scrape_team_stats(soup, match_id, team_id, side)

    # ── Player stats tables ─────────────────────────────────────────────────
    for team_id, side in [(home_id, "home"), (away_id, "away")]:
        _scrape_player_stats(soup, match_id, team_id, side)


def _scrape_team_stats(soup: BeautifulSoup, match_id: int,
                       team_id: int, side: str) -> None:
    """Parse and insert team-level match stats."""
    # FBref uses table ids like "stats_{team_id}_summary"
    # We find the relevant table by position (home=first, away=second)
    stats_tables = soup.select("table[id*='team_stats']")
    idx = 0 if side == "home" else 1
    if idx >= len(stats_tables):
        return

    tbl = stats_tables[idx]

    def stat(label: str) -> str | None:
        """Find a stat row by its label text."""
        for th in tbl.select("th"):
            if label.lower() in th.get_text().lower():
                td = th.find_next_sibling("td")
                return td.get_text(strip=True) if td else None
        return None

    row = {
        "match_id":          match_id,
        "team_id":           team_id,
        "possession_pct":    safe_float(stat("Possession")),
        "shots":             safe_int(stat("Shots")),
        "shots_on_target":   safe_int(stat("Shots on Target")),
        "corners":           safe_int(stat("Corners")),
        "fouls":             safe_int(stat("Fouls")),
        "yellow_cards":      safe_int(stat("Yellow Cards"), default=0),
        "red_cards":         safe_int(stat("Red Cards"), default=0),
        "offsides":          safe_int(stat("Offsides")),
        "source":            "fbref",
    }

    bulk_insert("match_stats", [row], conflict_cols=["match_id", "team_id"])


def _scrape_player_stats(soup: BeautifulSoup, match_id: int,
                         team_id: int, side: str) -> None:
    """Parse and insert player-level stats from the match report."""
    # Player stats tables: home summary is first, away is second
    player_tables = soup.select("table[id*='stats'][id*='summary']")
    idx = 0 if side == "home" else 1
    if idx >= len(player_tables):
        return

    tbl = player_tables[idx]
    rows = []

    for tr in tbl.select("tbody tr"):
        if "thead" in tr.get("class", []):
            continue
        cells = tr.select("td")
        if not cells:
            continue

        name_el = tr.select_one("td[data-stat='player'] a")
        if not name_el:
            continue

        player_name = clean_str(name_el.get_text())
        if not player_name:
            continue

        def cell_val(stat: str):
            el = tr.select_one(f"td[data-stat='{stat}']")
            return el.get_text(strip=True) if el else None

        pos_raw  = cell_val("position")
        position = normalise_position(pos_raw) if pos_raw else None
        minutes  = safe_int(cell_val("minutes"))

        player_id = get_or_create_player(
            player_name=player_name,
            position=position,
            source="fbref",
        )

        rows.append({
            "match_id":       match_id,
            "player_id":      player_id,
            "team_id":        team_id,
            "goals":          safe_int(cell_val("goals"), default=0),
            "assists":        safe_int(cell_val("assists"), default=0),
            "minutes_played": minutes,
            "was_starter":    (minutes or 0) > 45,
            "yellow_cards":   safe_int(cell_val("cards_yellow"), default=0),
            "red_cards":      safe_int(cell_val("cards_red"), default=0),
            "source":         "fbref",
        })

    bulk_insert("player_match_stats", rows, conflict_cols=["match_id", "player_id"])
    log.info("  Player stats: %d rows inserted for match %d (%s)", len(rows), match_id, side)


def run(years: list[int]) -> None:
    for year in years:
        scrape_tournament(year)
    log.info("FBref scraping complete for years: %s", years)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Scrape a specific year")
    parser.add_argument("--all", action="store_true", help="Scrape all configured years")
    args = parser.parse_args()

    if args.all:
        run(sorted(TOURNAMENT_URLS.keys()))
    elif args.year:
        run([args.year])
    else:
        parser.print_help()
