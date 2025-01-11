/*
    - A `datelist_int` generation query. 
    Convert the `device_activity_datelist` column into a `datelist_int` column 
*/


WITH users AS (
        SELECT 
            user_id
            , browser_type
            , device_activity_datelist
            , date
        FROM user_devices_cumulated
        --WHERE date = DATE('2023-01-01')
    )
,
    series AS (
        SELECT *
        FROM GENERATE_SERIES( DATE('2023-01-01'), DATE('2023-01-31')
            , INTERVAL '1 DAY') AS series_date
    )
,
    placeholder_ints AS(
        SELECT
            CASE WHEN
                device_activity_datelist @> ARRAY[DATE(series_date)]
                THEN CAST(POW(2, 31 - (date - DATE(series_date))) AS BIGINT)
                ELSE 0
                END AS placeholder_int_value
            , *
        FROM users 
        CROSS JOIN series       
        --WHERE user_id = '6499095000191390000' --'439578290726747300'
    )

    SELECT 
        user_id
        , browser_type
        , device_activity_datelist
        , date
        , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))
    FROM placeholder_ints
    GROUP BY 1, 2, 3, 4
    ORDER BY user_id, date

