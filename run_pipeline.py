"""
run_pipeline.py — Master orchestrator for the Epic 1 ingestion pipeline.

Runs all loaders and scrapers in the correct dependency order.
Safe to re-run — all inserts use ON CONFLICT DO NOTHING.

Usage:
    python run_pipeline.py                    # full pipeline
    python run_pipeline.py --phase kaggle     # bootstrap only
    python run_pipeline.py --phase fbref --year 2022
    python run_pipeline.py --phase transfermarkt --year 2022
    python run_pipeline.py --phase statsbomb --year 2022

Phases (in order):
    1. kaggle         — CSV bootstrap, no scraping needed
    2. transfermarkt  — player biographies and squad lists
    3. fbref          — match and player statistics
    4. statsbomb      — event-level data (2018, 2022 only)
"""
import sys
import logging
import argparse
from datetime import datetime

log = logging.getLogger("pipeline")


def setup_logging():
    import os
    from config import LOGS_DIR
    os.makedirs(LOGS_DIR, exist_ok=True)
    stamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOGS_DIR, f"pipeline_{stamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ]
    )
    log.info("Logging to %s", log_file)


def check_db_connection():
    from utils.db import get_connection
    try:
        conn = get_connection()
        conn.close()
        log.info("Database connection OK")
        return True
    except Exception as e:
        log.error("Cannot connect to database: %s", e)
        log.error("Check your .env file and ensure PostgreSQL is running.")
        return False


def phase_kaggle():
    log.info("=" * 60)
    log.info("PHASE 1 — Kaggle CSV bootstrap")
    log.info("=" * 60)
    from loaders.kaggle import run
    run()


def phase_transfermarkt(years: list[int]):
    log.info("=" * 60)
    log.info("PHASE 2 — Transfermarkt player & squad data")
    log.info("=" * 60)
    from scrapers.transfermarkt import run
    run(years)


def phase_fbref(years: list[int]):
    log.info("=" * 60)
    log.info("PHASE 3 — FBref match & player statistics")
    log.info("=" * 60)
    from scrapers.fbref import run
    run(years)


def phase_statsbomb(years: list[int]):
    log.info("=" * 60)
    log.info("PHASE 4 — StatsBomb event-level data")
    log.info("=" * 60)
    from loaders.statsbomb import run
    run(years)


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="World Cup 2026 data ingestion pipeline")
    parser.add_argument(
        "--phase",
        choices=["kaggle", "transfermarkt", "fbref", "statsbomb"],
        help="Run a single phase only (default: run all phases)"
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Limit scraping to a specific year (applies to fbref, transfermarkt, statsbomb)"
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help="Limit scraping to specific years e.g. --years 2018 2022"
    )
    args = parser.parse_args()

    # Default years for scrapers if none specified
    all_fbref_years   = [2022, 2018, 2014, 2010, 2006, 2002, 1998, 1994, 1990, 1986, 1982, 1978, 1974, 1970, 1966]
    all_tm_years      = [2022, 2018, 2014, 2010, 2006]
    all_sb_years      = [2022, 2018]

    if args.year:
        scrape_years = [args.year]
    elif args.years:
        scrape_years = args.years
    else:
        scrape_years = None  # use each phase's default

    if not check_db_connection():
        sys.exit(1)

    start = datetime.now()
    log.info("Pipeline started at %s", start.strftime("%Y-%m-%d %H:%M:%S"))

    try:
        if not args.phase or args.phase == "kaggle":
            phase_kaggle()

        if not args.phase or args.phase == "transfermarkt":
            phase_transfermarkt(scrape_years or all_tm_years)

        if not args.phase or args.phase == "fbref":
            phase_fbref(scrape_years or all_fbref_years)

        if not args.phase or args.phase == "statsbomb":
            phase_statsbomb(scrape_years or all_sb_years)

    except KeyboardInterrupt:
        log.warning("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        log.exception("Pipeline failed: %s", e)
        sys.exit(1)

    elapsed = datetime.now() - start
    log.info("Pipeline complete in %s", elapsed)


if __name__ == "__main__":
    main()
