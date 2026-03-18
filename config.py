"""
World Cup 2026 Prediction Engine — Pipeline Config
Copy .env.example to .env and fill in your values.
"""
import os
from dotenv import load_dotenv

load_dotenv()

DB = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "worldcup2026"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Scraping — be respectful to source sites
SCRAPE_DELAY_SECONDS = 2.5      # pause between requests
REQUEST_TIMEOUT      = 15       # seconds before giving up on a request
MAX_RETRIES          = 3        # retry failed requests this many times

# Paths
DATA_DIR   = os.path.join(os.path.dirname(__file__), "data")
KAGGLE_DIR = os.path.join(DATA_DIR, "kaggle")
RAW_DIR    = os.path.join(DATA_DIR, "raw")       # scraped JSON/HTML before cleaning
LOGS_DIR   = os.path.join(os.path.dirname(__file__), "logs")

# User-agent for scraping — identify yourself politely
USER_AGENT = (
    "WorldCup2026PredictionBot/1.0 "
    "(research project; contact via GitHub: MCCodes01)"
)
