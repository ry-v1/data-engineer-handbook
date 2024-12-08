/*
    - An incremental query that loads `host_activity_reduced`
    - day-by-day
*/

TRUNCATE TABLE host_activity_reduced
;

INSERT INTO host_activity_reduced
WITH yesterday AS (
    SELECT 
        host
        , hit_array
        , unique_visitors
        , month
    FROM host_activity_reduced
    WHERE month = '2023-01-01' --  '2022-12-31' -- 
    )
,
    today AS (
        SELECT DISTINCT
            host
            , CAST(COUNT(host) AS INT) AS hit_array
            , CAST(COUNT(DISTINCT user_id) AS INT) AS unique_visitors
            , DATE(event_time) AS date
        FROM events
        WHERE DATE(event_time) = '2023-01-03' -- '2023-01-02' --
            AND user_id IS NOT NULL
        GROUP BY host, DATE(event_time)
        )

    SELECT
        COALESCE(t.host, y.host) AS host
        , CASE 
            WHEN y.hit_array IS NOT NULL
                THEN y.hit_array ||  ARRAY[COALESCE(t.hit_array, 0)]
            WHEN y.hit_array IS NULL
                THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(t.hit_array, 0)]           
            END AS hit_array
        , CASE 
            WHEN y.unique_visitors IS NOT NULL
                THEN y.unique_visitors ||  ARRAY[COALESCE(t.unique_visitors, 0)]
            WHEN y.unique_visitors IS NULL
                THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(t.unique_visitors, 0)]           
            END AS unique_visitors
        , COALESCE(DATE_TRUNC('month', t.date), y.month) AS month
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.host = y.host
    ON CONFLICT (host, month)
    DO UPDATE SET unique_visitors = EXCLUDED.unique_visitors,
                    hit_array = EXCLUDED.hit_array
    ;

-- SELECT * FROM host_activity_reduced