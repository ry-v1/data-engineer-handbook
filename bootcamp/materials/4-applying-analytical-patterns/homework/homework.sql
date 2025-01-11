# Week 4 Applying Analytical Patterns
/*

The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

- A query that does state change tracking for `players`
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`

*/

DROP TABLE IF EXISTS players_state_tracking;

CREATE TABLE players_state_tracking(
    player_name text
    , first_active_season integer
    , last_active_season integer
    , player_state text
    , current_season integer
    , primary key (player_name, current_season)
    )
;

INSERT INTO players_state_tracking

WITH last_season AS (
    SELECT * 
    FROM players_state_tracking
    WHERE current_season = 1996
),
    this_season AS (
        SELECT * 
        FROM player_seasons
        WHERE season = 1997
    )

    SELECT 
        COALESCE(t.player_name, y.player_name) AS player_name
        , COALESCE(y.first_active_season, t.season) AS first_active_season
        , COALESCE(t.season, y.last_active_season)  AS last_active_season
        , CASE
            WHEN COALESCE(y.last_active_season, t.season) = t.season THEN 'New'
            WHEN COALESCE(y.last_active_season, t.season) = y.last_active_season THEN 'Retired'
            WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
            WHEN y.last_active_season < t.season - 1 THEN 'Returned from Retirement'
            ELSE 'Stayed Retired'
        END AS player_state
        , COALESCE(t.season, y.current_season + 1) AS current_season
    FROM this_season t
    FULL OUTER JOIN last_season y
    ON t.player_name = y.player_name
;

/* 

SELECT * FROM players_state_tracking
	
player_name first_active_season last_active_season  player_state  current_season
1	Aaron McKie	1997	1997	New	1997
2	Aaron Williams	1997	1997	New	1997
3	A.C. Green	1997	1997	New	1997

*/


/*
- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
*/

-- Answer questions like who scored the most points playing for one team?

WITH game_data AS (
    SELECT
        g.season
        , team_abbreviation as team
        , gd.player_name
        , gd.game_id
        , coalesce(gd.pts, 0) as game_pts
        , CASE
            WHEN gd.team_id = g.team_id_home THEN 1
            WHEN gd.team_id = g.team_id_away THEN 0
            ELSE null
          END AS winner
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.player_id 
            ORDER BY g.game_date_est) AS player_row_num
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id 
            ORDER BY g.game_date_est) AS team_row_num
    FROM game_details gd
    LEFT JOIN games g 
    ON gd.game_id = g.game_id
    WHERE player_name IS NOT NULL 
    )
,   
    game_agg_stats AS (
    	SELECT
    		season
    		, team
    		, player_name
    		, sum(game_pts) AS total_pts
    		, count(winner) AS total_wins
    	FROM game_data
    	WHERE player_row_num = 1
            AND player_name IS NOT NULL
    	GROUP BY
    		GROUPING SETS (
    			(player_name, team)
    		)
    )

    SELECT player_name, team, MAX(total_pts) AS total_pts
    FROM game_agg_stats
    WHERE player_name IS NOT NULL 
    GROUP BY team, player_name
    ORDER BY total_pts DESC
    ;

-- Giannis Antetokounmpo	MIL	15556

-- Answer questions like who scored the most points in one season?


WITH game_data AS (
    SELECT
        g.season
        , team_abbreviation as team
        , gd.player_name
        , gd.game_id
        , coalesce(gd.pts, 0) as game_pts
        , CASE
            WHEN gd.team_id = g.team_id_home THEN 1
            WHEN gd.team_id = g.team_id_away THEN 0
            ELSE null
          END AS winner
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.player_id 
            ORDER BY g.game_date_est) AS player_row_num
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id 
            ORDER BY g.game_date_est) AS team_row_num
    FROM game_details gd
    LEFT JOIN games g 
    ON gd.game_id = g.game_id
    WHERE player_name IS NOT NULL 
    )
,   
    game_agg_stats AS (
    	SELECT
    		season
    		, team
    		, player_name
    		, sum(game_pts) AS total_pts
    		, count(winner) AS total_wins
    	FROM game_data
    	WHERE player_row_num = 1
            AND player_name IS NOT NULL
    	GROUP BY
    		GROUPING SETS (
    			(player_name, season)
    		)
    )

    SELECT player_name,season, MAX(total_pts) AS total_pts
    FROM game_agg_stats
    WHERE season IS NOT NULL 
    GROUP BY player_name, season
    ORDER BY total_pts DESC
    ;
-- James Harden	2018	3247


-- Answer questions like which team has won the most games?

WITH game_data AS (
    SELECT
        g.season
        , team_abbreviation as team
        , gd.player_name
        , gd.game_id
        , coalesce(gd.pts, 0) as game_pts
        , CASE
            WHEN gd.team_id = g.team_id_home THEN 1
            WHEN gd.team_id = g.team_id_away THEN 0
            ELSE null
          END AS winner
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.player_id 
            ORDER BY g.game_date_est) AS player_row_num
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id 
            ORDER BY g.game_date_est) AS team_row_num
    FROM game_details gd
    LEFT JOIN games g 
    ON gd.game_id = g.game_id
    WHERE player_name IS NOT NULL 
    )
,   
    game_agg_stats AS (
    	SELECT
    		season
    		, team
    		, player_name
    		, sum(game_pts) AS total_pts
    		, count(winner) AS total_wins
    	FROM game_data
    	WHERE player_row_num = 1
            AND player_name IS NOT NULL
    	GROUP BY
    		GROUPING SETS (
    			(team)
    		)
    )

    SELECT team, sum(winner) AS total_team_pts
    FROM game_data
    WHERE team_row_num = 1
    GROUP BY team
    ORDER BY total_team_pts DESC
    ;
-- GSW,353


/*
- A query that uses window functions on `game_details` to find out the following things:
*/

-- What is the most games a team has won in a 90 game stretch? 

WITH game_data AS (
    SELECT
        g.season
        , team_abbreviation as team
        , gd.game_id
        , game_date_est
        , CASE
            WHEN gd.team_id = g.team_id_home THEN 1
            WHEN gd.team_id = g.team_id_away THEN 0
            ELSE null
        END AS winner
        , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id 
            ORDER BY g.game_date_est) as team_row_num
    FROM game_details gd
    LEFT JOIN games g 
    ON gd.game_id = g.game_id
)
SELECT team
       , game_date_est
       , SUM(winner) OVER (PARTITION BY team 
        ORDER BY game_date_est ROWS BETWEEN 89 PRECEDING AND CURRENT ROW)
FROM game_data
WHERE team_row_num = 1
ORDER BY sum(winner) OVER (PARTITION BY team 
    ORDER BY game_date_est ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) DESC

--PHI,2017-01-13,53

-- How many games in a row did LeBron James score over 10 points a game?

WITH count_pts AS (
    SELECT
        g.season
        , player_name
        , gd.game_id
        , game_date_est
        , pts
        , CASE WHEN pts > 10 THEN 1 ELSE 0 END as over_10_pts
        , ROW_NUMBER() OVER(PARTITION BY gd.player_id, gd.game_id 
            ORDER BY g.game_date_est) AS player_row_num
    FROM game_details gd
    LEFT JOIN games g 
    ON gd.game_id = g.game_id
)
, dedupe AS (
    SELECT season
        , player_name
        , game_id,pts
        , player_row_num
        , game_date_est
        , over_10_pts
        , row_number() OVER (PARTITION BY player_name 
            ORDER BY game_date_est) AS id
    FROM count_pts
    WHERE player_row_num = 1
    AND pts IS NOT NULL
    AND player_name = 'LeBron James'
), gap AS (
SELECT player_name
    ,game_date_est
    , id
    , over_10_pts
    , row_number() OVER (PARTITION BY over_10_pts ORDER BY game_date_est
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - id AS gap
    , id
    , row_number() OVER (PARTITION BY over_10_pts ORDER BY game_date_est
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM dedupe
WHERE over_10_pts = 1
ORDER BY game_date_est DESC)

SELECT player_name
       , count(1) AS streak_over_10pts
FROM gap
GROUP BY player_name, gap
ORDER BY count(1) DESC

--LeBron James,134
