DROP TYPE IF EXISTS season_stats CASCADE;
CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );

DROP TYPE IF EXISTS scoring_class CASCADE;
CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');

DROP TABLE IF EXISTS players;
CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );