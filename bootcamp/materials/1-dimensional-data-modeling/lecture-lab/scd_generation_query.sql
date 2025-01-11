INSERT INTO players_scd_table
WITH streak_started AS (
    SELECT player_name,
            current_season,
            scoring_class,
            is_active,
            LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> scoring_class
               OR LAG(scoring_class, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL
               OR LAG(is_active, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) <> is_active
               OR LAG(is_active, 1) OVER
               (PARTITION BY player_name ORDER BY current_season) IS NULL               
               AS did_change
    FROM players
),
     streak_identified AS (
         SELECT
            player_name,
                scoring_class,
                is_active,
                current_season,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            player_name,
            scoring_class,
            is_active,
            streak_identifier,
            MIN(current_season) AS start_date,
            MAX(current_season) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3,4
     )

     SELECT player_name, scoring_class, is_active, start_date, end_date
     FROM aggregated