/* 
    4. Backfill query for actors_history_scd: Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
*/

INSERT INTO actors_history_scd

WITH streak_started AS (
    SELECT 
        actor
        , year
        , quality_class
        , is_active
        , LAG(quality_class, 1) OVER
            (PARTITION BY actor ORDER BY year) <> quality_class
            OR LAG(quality_class, 1) OVER
            (PARTITION BY actor ORDER BY year) IS NULL
            AS quality_class_did_change
        , LAG(is_active, 1) OVER
            (PARTITION BY actor ORDER BY year) <> is_active
            OR LAG(is_active, 1) OVER
            (PARTITION BY actor ORDER BY year) IS NULL
            AS is_active_did_change    
    FROM actors
),
    streak_identified AS (
        SELECT 
            actor
            , year
            , quality_class
            , is_active
            , SUM(CASE WHEN quality_class_did_change OR is_active_did_change THEN 1 ELSE 0 END)
                    OVER (PARTITION BY actor ORDER BY year) as streak_identifier
        FROM streak_started
    
    ),
     aggregated AS (
         SELECT
            actor
            , quality_class
            , is_active
            , MIN(year) AS start_date
            , MAX(year) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3
    )

    SELECT actor, quality_class, is_active, start_date, end_date
    FROM aggregated
    ORDER BY 1,4,5,2,3
    ;

-- SELECT * FROM actors_history_scd
