/*
    - A cumulative query to generate `device_activity_datelist` from `events`
*/

/*
SELECT min(event_time), max(event_time) FROM events LIMIT 10;
SELECT * FROM events LIMIT 10;
SELECT * FROM devices LIMIT 10; WHERE user_id = '70132547320211180';

TRUNCATE TABLE user_devices_cumulated
;
    SELECT * 
        FROM events e
        JOIN devices d
        ON e.device_id = d.device_id 
*/

    INSERT INTO user_devices_cumulated
    WITH yesterday AS (
        SELECT 
            *
        FROM user_devices_cumulated
        WHERE date = '2023-01-01'
    ), today AS (
        SELECT 
            CAST(e.user_id AS TEXT) AS user_id
            , d.browser_type
            , MAX(DATE(CAST(e.event_time AS TIMESTAMP))) AS date_active
        FROM events e
        JOIN devices d
        ON e.device_id = d.device_id 
        WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = '2023-01-02'::date 
            AND e.user_id IS NOT NULL
            and d.browser_type IS NOT NULL
            --AND user_id = '743774307695414700	'
        GROUP BY 1, 2
    )
    SELECT 
        COALESCE (t.user_id, y.user_id) AS user_id
        , COALESCE (t.browser_type, y.browser_type) AS browser_type
        , CASE
            WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
            WHEN t.date_active IS NULL THEN y.device_activity_datelist
            ELSE ARRAY[t.date_active] || y.device_activity_datelist
        END AS dates_active
        , COALESCE (t.date_active, y.date + INTERVAL '1 day') AS date
    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
    ;

 /*    SELECT * FROM user_devices_cumulated 
    ORDER BY user_id, date */