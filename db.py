"""
utils/db.py — PostgreSQL connection and helper functions.

All inserts use ON CONFLICT DO NOTHING (upsert-safe) so the
pipeline can be re-run without creating duplicates.
"""
import logging
import psycopg2
import psycopg2.extras
from config import DB

log = logging.getLogger(__name__)


def get_connection():
    """Return a live psycopg2 connection using settings from config.py."""
    return psycopg2.connect(**DB)


def execute(sql: str, params=None, conn=None):
    """
    Run a single statement. Opens and closes its own connection
    unless one is passed in (useful for transactions).
    """
    owned = conn is None
    if owned:
        conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        if owned:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if owned:
            conn.close()


def bulk_insert(table: str, rows: list[dict], conflict_cols: list[str], conn=None):
    """
    Insert a list of dicts into `table`.
    Skips rows that conflict on `conflict_cols` — safe to re-run.

    Example:
        bulk_insert("teams", [{"country": "Brazil", "fifa_code": "BRA"}],
                    conflict_cols=["country"])
    """
    if not rows:
        log.debug("bulk_insert called with empty rows for %s — skipping", table)
        return 0

    cols    = list(rows[0].keys())
    conflict = ", ".join(conflict_cols)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names    = ", ".join(cols)

    sql = (
        f"INSERT INTO {table} ({col_names}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict}) DO NOTHING"
    )

    owned = conn is None
    if owned:
        conn = get_connection()

    inserted = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                values = [row[c] for c in cols]
                cur.execute(sql, values)
                inserted += cur.rowcount
        if owned:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if owned:
            conn.close()

    log.info("bulk_insert: %d/%d rows inserted into %s", inserted, len(rows), table)
    return inserted


def fetch_one(sql: str, params=None):
    """Return a single row as a dict, or None."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def fetch_all(sql: str, params=None) -> list[dict]:
    """Return all rows as a list of dicts."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def get_or_create_team(country: str, fifa_code: str = None,
                       confederation: str = None, conn=None) -> int:
    """
    Return the team_id for `country`, inserting if it doesn't exist.
    Useful during ingestion when we encounter a team name in the data.
    """
    row = fetch_one("SELECT team_id FROM teams WHERE country = %s", (country,))
    if row:
        return row["team_id"]

    owned = conn is None
    if owned:
        conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teams (country, fifa_code, confederation)
                VALUES (%s, %s, %s)
                ON CONFLICT (country) DO UPDATE SET country = EXCLUDED.country
                RETURNING team_id
                """,
                (country, fifa_code, confederation),
            )
            team_id = cur.fetchone()[0]
        if owned:
            conn.commit()
        return team_id
    except Exception:
        conn.rollback()
        raise
    finally:
        if owned:
            conn.close()


def get_or_create_player(player_name: str, nationality: str = None,
                          position: str = None, dob=None,
                          source: str = None, conn=None) -> int:
    """
    Return player_id for `player_name` + `nationality`, inserting if needed.
    NOTE: player names are not globally unique — the same name can exist for
    different nationalities. Always pass nationality when you have it.
    """
    row = fetch_one(
        "SELECT player_id FROM players WHERE player_name = %s AND nationality IS NOT DISTINCT FROM %s",
        (player_name, nationality),
    )
    if row:
        return row["player_id"]

    owned = conn is None
    if owned:
        conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO players (player_name, nationality, position, date_of_birth, source)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING player_id
                """,
                (player_name, nationality, position, dob, source),
            )
            player_id = cur.fetchone()[0]
        if owned:
            conn.commit()
        return player_id
    except Exception:
        conn.rollback()
        raise
    finally:
        if owned:
            conn.close()


def get_tournament_id(year: int) -> int | None:
    """Return tournament_id for a given year, or None if not found."""
    row = fetch_one("SELECT tournament_id FROM tournaments WHERE year = %s", (year,))
    return row["tournament_id"] if row else None
