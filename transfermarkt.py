"""
scrapers/transfermarkt.py — Player biographies, club histories, squad lists.

Transfermarkt is the definitive source for player data. Pages are
JS-rendered so we use Playwright (headless Chromium). Install with:
    pip install playwright && playwright install chromium

Run:
    python -m scrapers.transfermarkt --year 2022
    python -m scrapers.transfermarkt --all
"""
import time
import logging
import argparse
import json
import os
from datetime import date

from utils.db import (
    bulk_insert, get_or_create_team, get_or_create_player,
    get_tournament_id, fetch_all
)
from utils.clean import (
    normalise_country, normalise_position, safe_int,
    clean_str, parse_date
)
from config import SCRAPE_DELAY_SECONDS, RAW_DIR

log = logging.getLogger(__name__)

# Transfermarkt World Cup squad pages by year
SQUAD_URLS = {
    2022: "https://www.transfermarkt.com/weltmeisterschaft/teilnehmer/pokalwettbewerb/WM/saison_id/2022",
    2018: "https://www.transfermarkt.com/weltmeisterschaft/teilnehmer/pokalwettbewerb/WM/saison_id/2018",
    2014: "https://www.transfermarkt.com/weltmeisterschaft/teilnehmer/pokalwettbewerb/WM/saison_id/2014",
    2010: "https://www.transfermarkt.com/weltmeisterschaft/teilnehmer/pokalwettbewerb/WM/saison_id/2010",
    2006: "https://www.transfermarkt.com/weltmeisterschaft/teilnehmer/pokalwettbewerb/WM/saison_id/2006",
}


def _get_browser_page():
    """
    Launch a headless Playwright browser page.
    Returns (browser, page) — caller must close the browser.
    """
    try:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        # Block images and fonts to speed up loading
        page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2}", lambda r: r.abort())
        return pw, browser, page
    except ImportError:
        log.error(
            "Playwright not installed. Run: pip install playwright && playwright install chromium"
        )
        raise


def _fetch_page(page, url: str) -> str | None:
    """Navigate to URL and return page HTML, with rate limiting."""
    time.sleep(SCRAPE_DELAY_SECONDS)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(1500)  # let JS settle
        return page.content()
    except Exception as e:
        log.error("Failed to fetch %s: %s", url, e)
        return None


def scrape_tournament_squads(year: int) -> None:
    """
    Scrape all squad lists for a given World Cup year.
    Populates: players, tournament_squads, and club_team in player_careers.
    """
    if year not in SQUAD_URLS:
        log.error("No Transfermarkt URL configured for year %d", year)
        return

    t_id = get_tournament_id(year)
    if not t_id:
        log.error("Tournament %d not in database — run Kaggle loader first", year)
        return

    log.info("Scraping Transfermarkt squads for World Cup %d...", year)

    pw, browser, page = _get_browser_page()
    try:
        from bs4 import BeautifulSoup

        # Step 1: get list of participating teams
        html = _fetch_page(page, SQUAD_URLS[year])
        if not html:
            return

        soup = BeautifulSoup(html, "html.parser")
        team_links = []
        for a in soup.select("table.items tbody tr td.hauptlink a[href*='/startseite/']"):
            href = a.get("href", "")
            country = clean_str(a.get_text())
            if href and country:
                team_links.append((country, "https://www.transfermarkt.com" + href))

        log.info("Found %d teams for %d", len(team_links), year)

        for country, team_url in team_links:
            # Build squad URL from team URL by replacing 'startseite' with 'kader'
            squad_url = team_url.replace("/startseite/", "/kader/") + f"/saison_id/{year}"
            _scrape_team_squad(page, squad_url, country, year, t_id)

    finally:
        browser.close()
        pw.stop()


def _scrape_team_squad(page, squad_url: str, country: str,
                        year: int, tournament_id: int) -> None:
    """Scrape a single team's squad page and insert into DB."""
    from bs4 import BeautifulSoup

    log.info("  Scraping squad: %s %d", country, year)
    html = _fetch_page(page, squad_url)
    if not html:
        return

    soup  = BeautifulSoup(html, "html.parser")
    country_norm = normalise_country(country)
    team_id = get_or_create_team(country_norm)

    squad_rows   = []
    career_rows  = []
    player_rows_inserted = 0

    for tr in soup.select("table.items tbody tr.odd, table.items tbody tr.even"):
        cells = tr.select("td")
        if len(cells) < 5:
            continue

        # Player name and link
        name_el = tr.select_one("td.hauptlink a")
        if not name_el:
            continue
        player_name = clean_str(name_el.get_text())
        if not player_name:
            continue

        # Position
        pos_el  = tr.select_one("td.posrela table td")
        pos_raw = clean_str(pos_el.get_text()) if pos_el else None
        position = normalise_position(pos_raw) if pos_raw else None

        # Date of birth — usually in a td after position
        dob_el  = tr.select_one("td[class*='zentriert']")
        dob     = parse_date(dob_el.get_text(strip=True)) if dob_el else None

        # Shirt number
        num_el      = tr.select_one("div.rn_nummer")
        shirt_num   = safe_int(num_el.get_text()) if num_el else None

        # Club at time of tournament
        club_el = tr.select_one("td.hauptlink.no-border-links a")
        club    = clean_str(club_el.get_text()) if club_el else None

        # Caps (international appearances before this tournament)
        caps_el = None
        for td in tr.select("td.zentriert"):
            text = td.get_text(strip=True)
            if text.isdigit() and int(text) < 300:
                caps_el = text
                break
        caps = safe_int(caps_el)

        player_id = get_or_create_player(
            player_name=player_name,
            nationality=country_norm,
            position=position,
            dob=dob,
            source="transfermarkt",
        )

        squad_rows.append({
            "tournament_id":  tournament_id,
            "team_id":        team_id,
            "player_id":      player_id,
            "shirt_number":   shirt_num,
            "position":       position,
            "club_team":      club,
            "caps_before_wc": caps,
            "source":         "transfermarkt",
        })

        # Update player_careers with club info if a career row exists
        career_rows.append({
            "player_id":    player_id,
            "tournament_id": tournament_id,
            "team_id":       team_id,
            "club_team":     club,
            "source":        "transfermarkt",
        })

        player_rows_inserted += 1

    bulk_insert("tournament_squads", squad_rows,
                conflict_cols=["tournament_id", "team_id", "player_id"])

    # Upsert club_team into player_careers (may already exist from Kaggle)
    from utils.db import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for r in career_rows:
                cur.execute(
                    """
                    INSERT INTO player_careers
                        (player_id, tournament_id, team_id, club_team, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (player_id, tournament_id)
                    DO UPDATE SET club_team = EXCLUDED.club_team,
                                  source    = EXCLUDED.source
                    """,
                    (r["player_id"], r["tournament_id"], r["team_id"],
                     r["club_team"], r["source"]),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("  %s %d: %d players inserted/updated", country_norm, year, player_rows_inserted)


def run(years: list[int]) -> None:
    for year in years:
        scrape_tournament_squads(year)
    log.info("Transfermarkt scraping complete for years: %s", years)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.all:
        run(sorted(SQUAD_URLS.keys()))
    elif args.year:
        run([args.year])
    else:
        parser.print_help()
