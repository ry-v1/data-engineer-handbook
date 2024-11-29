/*
    - The incremental query to generate `host_activity_datelist`
*/

TRUNCATE TABLE hosts_cumulated
;

INSERT INTO hosts_cumulated

WITH yesterday AS (
    SELECT 
        host
        , host_activity_datelist
        , date
    FROM hosts_cumulated
    WHERE date = '2023-01-02') --  '2022-12-31') --
,
    today AS (
        SELECT DISTINCT
            host
            , DATE(event_time) AS date
        FROM events
        WHERE DATE(event_time) = '2023-01-03')

    SELECT
        COALESCE(t.host, y.host) AS host
        , CASE 
            WHEN t.host IS NOT NULL THEN ARRAY[t.date] || y.host_activity_datelist
            --WHEN y.host IS NULL THEN ARRAY[]::date[]
            ELSE ARRAY[]::date[] || y.host_activity_datelist
            END AS host_activity_datelist
        , COALESCE(t.date, y.date) AS date
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.host = y.host
    ;

-- SELECT * FROM hosts_cumulated