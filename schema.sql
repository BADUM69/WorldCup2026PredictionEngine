-- =============================================================================
-- World Cup 2026 Prediction Engine
-- Story 1.1 — PostgreSQL Schema Design
-- Epic 1: Data Gathering
-- =============================================================================
-- Safe to re-run: all objects use DROP IF EXISTS before creation
-- =============================================================================

-- =============================================================================
-- CLEANUP (safe re-run)
-- =============================================================================

DROP TABLE IF EXISTS tournament_squads    CASCADE;
DROP TABLE IF EXISTS player_careers       CASCADE;
DROP TABLE IF EXISTS player_match_stats   CASCADE;
DROP TABLE IF EXISTS match_stats          CASCADE;
DROP TABLE IF EXISTS matches              CASCADE;
DROP TABLE IF EXISTS players              CASCADE;
DROP TABLE IF EXISTS teams                CASCADE;
DROP TABLE IF EXISTS tournaments          CASCADE;

DROP TYPE IF EXISTS match_stage;
DROP TYPE IF EXISTS confederation;
DROP TYPE IF EXISTS player_position;
DROP TYPE IF EXISTS data_source;

-- =============================================================================
-- ENUMS
-- =============================================================================

CREATE TYPE match_stage AS ENUM (
    'Group',
    'Round of 16',
    'Quarter-final',
    'Semi-final',
    'Third place',
    'Final'
);

CREATE TYPE confederation AS ENUM (
    'UEFA', 'CONMEBOL', 'CONCACAF', 'CAF', 'AFC', 'OFC'
);

CREATE TYPE player_position AS ENUM (
    'GK', 'DF', 'MF', 'FW'
);

-- Tracks which source each ingested row came from (Story 1.4 quality report)
CREATE TYPE data_source AS ENUM (
    'statsbomb',
    'fbref',
    'fifa_api',
    'transfermarkt',
    'kaggle',
    'wikipedia',
    'manual'
);

-- =============================================================================
-- TABLE 1: tournaments
-- One row per World Cup edition (1930 – 2026)
-- =============================================================================

CREATE TABLE tournaments (
    tournament_id       SERIAL          PRIMARY KEY,
    year                INT             NOT NULL UNIQUE,
    host_country        VARCHAR(100)    NOT NULL,
    host_note           VARCHAR(200),
    winner_team_id      INT,
    runner_up_team_id   INT,
    num_teams           INT             NOT NULL,
    total_matches       INT,
    total_goals         INT,
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE tournaments IS 'One row per FIFA World Cup edition, 1930 to present.';
COMMENT ON COLUMN tournaments.winner_team_id IS 'FK to teams. NULL for future or in-progress tournaments.';
COMMENT ON COLUMN tournaments.host_note IS 'Free-text note for multi-country hosts, e.g. 2026 USA/CAN/MEX.';

-- =============================================================================
-- TABLE 2: teams
-- National teams that have appeared in at least one World Cup
-- =============================================================================

CREATE TABLE teams (
    team_id             SERIAL          PRIMARY KEY,
    country             VARCHAR(100)    NOT NULL UNIQUE,
    fifa_code           CHAR(3),                            -- e.g. BRA, FRA, ARG
    confederation       confederation,
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE teams IS 'National teams that have appeared in at least one World Cup.';
COMMENT ON COLUMN teams.fifa_code IS '3-letter FIFA country code.';

-- =============================================================================
-- TABLE 3: players
-- Individual player registry across all tournaments
-- =============================================================================

CREATE TABLE players (
    player_id           SERIAL          PRIMARY KEY,
    player_name         VARCHAR(150)    NOT NULL,
    nationality         VARCHAR(100),                       -- primary nationality for display
    position            player_position,
    date_of_birth       DATE,
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE players IS 'Master player registry. Team association is via tournament_squads, not stored here directly — players can represent different nations across eras.';

CREATE INDEX idx_players_name ON players(player_name);

-- =============================================================================
-- TABLE 4: matches
-- One row per match played at any World Cup
-- =============================================================================

CREATE TABLE matches (
    match_id            SERIAL          PRIMARY KEY,
    tournament_id       INT             NOT NULL REFERENCES tournaments(tournament_id) ON DELETE CASCADE,
    home_team_id        INT             NOT NULL REFERENCES teams(team_id),
    away_team_id        INT             NOT NULL REFERENCES teams(team_id),
    stage               match_stage     NOT NULL,
    match_date          DATE,
    venue               VARCHAR(150),
    city                VARCHAR(100),
    attendance          INT,
    home_score          INT,
    away_score          INT,
    home_score_aet      INT,
    away_score_aet      INT,
    home_score_pens     INT,
    away_score_pens     INT,
    went_to_extra_time  BOOLEAN         DEFAULT FALSE,
    went_to_penalties   BOOLEAN         DEFAULT FALSE,
    referee             VARCHAR(150),
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),

    CONSTRAINT different_teams      CHECK (home_team_id <> away_team_id),
    CONSTRAINT valid_home_score     CHECK (home_score >= 0),
    CONSTRAINT valid_away_score     CHECK (away_score >= 0),
    CONSTRAINT valid_attendance     CHECK (attendance >= 0),
    CONSTRAINT pens_require_aet     CHECK (
        went_to_penalties = FALSE OR went_to_extra_time = TRUE
    )
);

COMMENT ON TABLE matches IS 'One row per match. home/away_score are 90-min scores. AET and penalty scores only populated when applicable.';

CREATE INDEX idx_matches_tournament ON matches(tournament_id);
CREATE INDEX idx_matches_home_team  ON matches(home_team_id);
CREATE INDEX idx_matches_away_team  ON matches(away_team_id);
CREATE INDEX idx_matches_stage      ON matches(stage);

-- =============================================================================
-- TABLE 5: match_stats
-- Team-level statistics per match (possession, shots, etc.)
-- =============================================================================

CREATE TABLE match_stats (
    stat_id             SERIAL          PRIMARY KEY,
    match_id            INT             NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    team_id             INT             NOT NULL REFERENCES teams(team_id),
    possession_pct      NUMERIC(4,1),
    shots               INT,
    shots_on_target     INT,
    corners             INT,
    fouls               INT,
    yellow_cards        INT             DEFAULT 0,
    red_cards           INT             DEFAULT 0,
    offsides            INT,
    passes              INT,
    pass_accuracy_pct   NUMERIC(4,1),
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),

    CONSTRAINT unique_match_team        UNIQUE (match_id, team_id),
    CONSTRAINT valid_possession         CHECK (possession_pct BETWEEN 0 AND 100),
    CONSTRAINT shots_on_target_lte_shots CHECK (
        shots_on_target IS NULL OR shots IS NULL OR shots_on_target <= shots
    )
);

COMMENT ON TABLE match_stats IS 'Team-level match stats. Two rows per match (one per team). Available primarily post-1966.';

CREATE INDEX idx_match_stats_match  ON match_stats(match_id);
CREATE INDEX idx_match_stats_team   ON match_stats(team_id);

-- =============================================================================
-- TABLE 6: player_match_stats
-- Individual player statistics per match
-- =============================================================================

CREATE TABLE player_match_stats (
    pms_id              SERIAL          PRIMARY KEY,
    match_id            INT             NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    player_id           INT             NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
    team_id             INT             NOT NULL REFERENCES teams(team_id),
    goals               INT             DEFAULT 0,
    assists             INT             DEFAULT 0,
    minutes_played      INT,
    was_starter         BOOLEAN         DEFAULT TRUE,
    subbed_on_minute    INT,
    subbed_off_minute   INT,
    yellow_cards        INT             DEFAULT 0,
    red_cards           INT             DEFAULT 0,
    distance_km         NUMERIC(4,1),
    sprints             INT,
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),

    CONSTRAINT unique_player_match      UNIQUE (match_id, player_id),
    CONSTRAINT valid_minutes            CHECK (minutes_played BETWEEN 0 AND 120),
    CONSTRAINT valid_subbed_on          CHECK (subbed_on_minute IS NULL OR subbed_on_minute BETWEEN 0 AND 120),
    CONSTRAINT valid_subbed_off         CHECK (subbed_off_minute IS NULL OR subbed_off_minute BETWEEN 0 AND 120),
    CONSTRAINT starter_no_sub_on        CHECK (was_starter = FALSE OR subbed_on_minute IS NULL),
    CONSTRAINT non_negative_goals       CHECK (goals >= 0),
    CONSTRAINT non_negative_assists     CHECK (assists >= 0)
);

COMMENT ON TABLE player_match_stats IS 'Player stats per match. Physical stats (distance, sprints) only available from 2014 onwards.';

CREATE INDEX idx_pms_match   ON player_match_stats(match_id);
CREATE INDEX idx_pms_player  ON player_match_stats(player_id);
CREATE INDEX idx_pms_team    ON player_match_stats(team_id);

-- =============================================================================
-- TABLE 7: player_careers
-- Aggregated career stats per player per tournament
-- =============================================================================

CREATE TABLE player_careers (
    career_id           SERIAL          PRIMARY KEY,
    player_id           INT             NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
    tournament_id       INT             NOT NULL REFERENCES tournaments(tournament_id) ON DELETE CASCADE,
    team_id             INT             NOT NULL REFERENCES teams(team_id),
    club_team           VARCHAR(150),
    appearances         INT             DEFAULT 0,
    goals               INT             DEFAULT 0,
    assists             INT             DEFAULT 0,
    minutes_played      INT             DEFAULT 0,
    yellow_cards        INT             DEFAULT 0,
    red_cards           INT             DEFAULT 0,
    was_captain         BOOLEAN         DEFAULT FALSE,
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),

    CONSTRAINT unique_player_tournament UNIQUE (player_id, tournament_id),
    CONSTRAINT non_negative_appearances CHECK (appearances >= 0),
    CONSTRAINT non_negative_goals       CHECK (goals >= 0),
    CONSTRAINT non_negative_assists     CHECK (assists >= 0),
    CONSTRAINT non_negative_minutes     CHECK (minutes_played >= 0)
);

COMMENT ON TABLE player_careers IS 'Aggregated stats per player per tournament. Age is computed via JOIN to players.date_of_birth + tournaments.year — not stored to avoid inconsistency.';

CREATE INDEX idx_careers_player     ON player_careers(player_id);
CREATE INDEX idx_careers_tournament ON player_careers(tournament_id);
CREATE INDEX idx_careers_team       ON player_careers(team_id);

-- =============================================================================
-- TABLE 8: tournament_squads
-- The official squad list submitted by each team per tournament
-- =============================================================================

CREATE TABLE tournament_squads (
    squad_id            SERIAL          PRIMARY KEY,
    tournament_id       INT             NOT NULL REFERENCES tournaments(tournament_id) ON DELETE CASCADE,
    team_id             INT             NOT NULL REFERENCES teams(team_id),
    player_id           INT             NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
    shirt_number        INT,
    position            player_position,
    club_team           VARCHAR(150),
    caps_before_wc      INT,
    source              data_source,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),

    CONSTRAINT unique_squad_entry   UNIQUE (tournament_id, team_id, player_id),
    CONSTRAINT valid_shirt_number   CHECK (shirt_number BETWEEN 1 AND 99)
);

COMMENT ON TABLE tournament_squads IS 'Official squad list per team per tournament. Distinct from player_careers — a player can be named in a squad without playing. Age computed via JOIN to players.date_of_birth.';

CREATE INDEX idx_squads_tournament  ON tournament_squads(tournament_id);
CREATE INDEX idx_squads_team        ON tournament_squads(team_id);
CREATE INDEX idx_squads_player      ON tournament_squads(player_id);

-- =============================================================================
-- DEFERRED FOREIGN KEYS
-- winner_team_id and runner_up_team_id on tournaments reference teams,
-- but teams is created after tournaments to keep dependency order clean.
-- We add the FKs here once both tables exist.
-- =============================================================================

ALTER TABLE tournaments
    ADD CONSTRAINT fk_winner    FOREIGN KEY (winner_team_id)    REFERENCES teams(team_id) ON DELETE SET NULL,
    ADD CONSTRAINT fk_runner_up FOREIGN KEY (runner_up_team_id) REFERENCES teams(team_id) ON DELETE SET NULL;

-- =============================================================================
-- SAMPLE SEED DATA (for testing schema only — Story 1.3 loads full data)
-- =============================================================================

INSERT INTO tournaments (year, host_country, host_note, num_teams, source)
VALUES
    (2022, 'Qatar',         NULL,                                   32, 'kaggle'),
    (2018, 'Russia',        NULL,                                   32, 'kaggle'),
    (2014, 'Brazil',        NULL,                                   32, 'kaggle'),
    (2026, 'United States', 'Co-hosted by USA, Canada and Mexico',  48,  NULL);

INSERT INTO teams (country, fifa_code, confederation)
VALUES
    ('Argentina', 'ARG', 'CONMEBOL'),
    ('France',    'FRA', 'UEFA'),
    ('Germany',   'GER', 'UEFA'),
    ('Brazil',    'BRA', 'CONMEBOL'),
    ('Croatia',   'CRO', 'UEFA');

-- Wire up winners now that teams exist
UPDATE tournaments SET winner_team_id    = (SELECT team_id FROM teams WHERE fifa_code = 'ARG') WHERE year = 2022;
UPDATE tournaments SET runner_up_team_id = (SELECT team_id FROM teams WHERE fifa_code = 'FRA') WHERE year = 2022;
UPDATE tournaments SET winner_team_id    = (SELECT team_id FROM teams WHERE fifa_code = 'FRA') WHERE year = 2018;
UPDATE tournaments SET runner_up_team_id = (SELECT team_id FROM teams WHERE fifa_code = 'CRO') WHERE year = 2018;
UPDATE tournaments SET winner_team_id    = (SELECT team_id FROM teams WHERE fifa_code = 'GER') WHERE year = 2014;
UPDATE tournaments SET runner_up_team_id = (SELECT team_id FROM teams WHERE fifa_code = 'ARG') WHERE year = 2014;
