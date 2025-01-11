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
            , COUNT(host) AS hit_array
            , ARRAY[COUNT(DISTINCT user_id)] AS unique_visitors
            , DATE_TRUNC('MONTH', event_time::TIMESTAMP) AS month
        FROM events
        WHERE DATE(event_time) = '2023-01-02'
            AND user_id IS NOT NULL
        GROUP BY host, DATE_TRUNC('MONTH', event_time::TIMESTAMP)
        )

    SELECT
        COALESCE(t.host, y.host) AS host
        , COALESCE(t.hit_array, y.hit_array) AS hit_array
        , CASE 
            WHEN t.host IS NOT NULL THEN ARRAY[t.unique_visitors] || y.unique_visitors
            --WHEN y.host IS NULL THEN ARRAY[]::date[]
            ELSE ARRAY[]::INTEGER[] || y.unique_visitors
            END AS unique_visitors
        , COALESCE(t.month, y.month) AS month
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.host = y.host
    ;

-- SELECT * FROM host_activity_reduced