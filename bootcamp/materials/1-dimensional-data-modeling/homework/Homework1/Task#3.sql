/*
    3. DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:
        - Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
        - Tracks quality_class and is_active status for each actor in the actors table.
*/

DROP TABLE IF EXISTS actors_history_scd
;

CREATE TABLE actors_history_scd (
            actor text
            , quality_class quality_class_type
            , is_active boolean
            , start_date integer
            , end_date integer)
;
