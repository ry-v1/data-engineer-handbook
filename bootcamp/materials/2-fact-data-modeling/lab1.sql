--select min(event_time), max(event_time) from events
DROP TABLE IF EXISTS fct_game_details 
;

CREATE TABLE fct_game_details(
    dim_game_date DATE,
    , dim_season INTEGER
    , dim_team_id INTEGER
    , dim_player_id INTEGER
    , dim_player_name TEXT
    , dim_start_position TEXT
    , dim_is_playing_at_home BOOLEAN
    , dim_did_not_play BOOLEAN
    , dim_did_not_dress BOOLEAN
    , dim_did_not_with_team BOOLEAN
    , m_minutes REAL,
    , m_fgm INTEGER
    , m_fga INTEGER
    , m_fg3m INTEGER
    , m_fg3a INTEGER
    , m_ftm INTEGER
    , m_fta INTEGER
    , m_ored INTEGER
    , m_dreb INTEGER
    , m_reb INTEGER
    , m_ast INTEGER
    , m_stl INTEGER
    , m_blk INTEGER
    , m_turnovers INTEGER
    , m_pf INTEGER
    , m_pts INTEGER
    , m_plus_minus INTEGER
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)) 
;
/*        , team_abbreviation
        , team_city
        , player_id
        , player_name
        , nickname
        , start_position
        , comment
        , min
        , fgm
        , fga
        , fg_pct
        , fg3m
        , fg3a
        , fg3_pct
        , ftm
        , fta
        , ft_pct
        , oreb
        , dreb
        , reb
        , ast
        , stl
        , blk
        , TO
        , pf
        , pts
        , team_id
        , plus_minus */
        
WITH deduped AS(
    SELECT gd.*
        , g.game_date_est
        , g.season
        , g.home_team_id
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    -- where g.game_date_est = '2016-01-01'
)
;

INSERT INTO fct_game_details

SELECT game_date_est AS dim_game_date,
    , season AS dim_season,
    , team_id AS dim_team_id,
    , player_id AS dim_plyer_id,
    , player_name AS dim_player_name,
    , start_position AS dim_start_position,
    , team_id = home_team_id AS dim_is_playing_at_home,
    , COALESCE(POSITION('DNP' IN comment), 0) > 0 AS dim_did_not_play,
    , COALESCE(POSITION('DND' IN comment), 0) > 0 AS dim_did_not_dress,
    , COALESCE(POSITION('NWT' IN comment), 0) > 0 AS dim_did_not_with_team,
    , cast(SPLIT_PART(min, ':', 1) AS REAL) + cast(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minuties,
    , fgm AS m_fgm,
    , fga AS m_fga,
    , fg3m AS m_fg3m,
    , fg3a AS m_fg3a,
    , ftm AS m_ftm,
    , fta AS m_fta,
    , oreb AS m_ored,
    , dreb AS m_dreb,
    , reb AS m_reb,
    , ast AS m_ast,
    , stl AS m_stl,
    , blk AS m_blk,
    , TO AS m_turnovers,
    , pf AS m_pf,
    , pts AS m_pts,
    , plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1
;

SELECT t.*, gd.* 
FROM fct_game_details gd 
JOIN teams t
ON t.team_id = gd.dim_team_id
;

SELECT dim_player_name
, dim_is_playing_at_home
, COUNT(1) AS num_games
, SUM(m_pts) AS total_points
, COUNT(CASE WHEN dim_did_not_with_team THEN 1 END) AS bailed_num
, CAST(COUNT(CASE WHEN dim_did_not_with_team THEN 1 END) AS REAL) / count(1) AS bailed_pct
FROM fct_game_details
GROUP BY 1, 2
ORDER BY 6 DESC