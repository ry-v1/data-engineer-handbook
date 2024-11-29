/*
    - A monthly, reduced fact table DDL `host_activity_reduced`
    - month
    - host
    - hit_array - think COUNT(1)
    - unique_visitors array -  think COUNT(DISTINCT user_id)
*/

DROP TABLE IF EXISTS host_activity_reduced
;

CREATE TABLE host_activity_reduced(
    host TEXT
    , hit_array INTEGER[]
    , unique_visitors INTEGER[]
    , month DATE
    , PRIMARY KEY(host, month)
    )
;
