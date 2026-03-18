# Epic 1 — Data Ingestion Pipeline

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt
playwright install chromium

# 2. Configure database
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 3. Deploy the schema (Story 1.1)
psql -U postgres -d worldcup2026 -f ../schema.sql
```

## Getting the data

### Kaggle (Phase 1 — do this first)
1. Download from https://www.kaggle.com/datasets/abecklas/fifa-world-cup
2. Place `WorldCups.csv`, `WorldCupMatches.csv`, `WorldCupPlayers.csv` in `data/kaggle/`

### StatsBomb (Phase 4)
```bash
git clone https://github.com/statsbomb/open-data.git data/statsbomb
```

FBref and Transfermarkt are scraped automatically.

## Running the pipeline

```bash
# Full pipeline (runs all 4 phases in order)
python run_pipeline.py

# Bootstrap only — fastest, no scraping
python run_pipeline.py --phase kaggle

# Single phase, single year
python run_pipeline.py --phase fbref --year 2022
python run_pipeline.py --phase transfermarkt --year 2022

# Multiple specific years
python run_pipeline.py --phase fbref --years 2018 2022
```

## Pipeline phases

| Phase | Script | Source | Tables populated |
|-------|--------|--------|-----------------|
| 1 | `loaders/kaggle.py` | Kaggle CSVs | tournaments, teams, matches, player_careers |
| 2 | `scrapers/transfermarkt.py` | Transfermarkt | players, tournament_squads, player_careers |
| 3 | `scrapers/fbref.py` | FBref | matches, match_stats, player_match_stats |
| 4 | `loaders/statsbomb.py` | StatsBomb | match_stats, player_match_stats |

All phases are **safe to re-run** — inserts use `ON CONFLICT DO NOTHING`.

## Logs

Each run writes a timestamped log file to `logs/`. Check these for errors,
skipped rows, and ingestion counts. These logs feed directly into the
Story 1.4 data quality report.

## Project structure

```
pipeline/
├── config.py               # DB connection, scraping settings
├── .env.example            # Copy to .env and fill in credentials
├── run_pipeline.py         # Master orchestrator
├── requirements.txt
├── loaders/
│   ├── kaggle.py           # Phase 1: CSV bootstrap
│   └── statsbomb.py        # Phase 4: event-level JSON
├── scrapers/
│   ├── fbref.py            # Phase 3: match & player stats
│   └── transfermarkt.py    # Phase 2: player biographies & squads
├── utils/
│   ├── db.py               # DB connection, bulk_insert, helpers
│   └── clean.py            # Normalisation, parsing, cleaning
└── data/
    ├── kaggle/             # Place Kaggle CSVs here
    ├── statsbomb/          # git clone open-data here
    └── raw/                # Intermediate scraped files
```
