/*
    1. DDL for actors table: Create a DDL for an actors table with the following fields:

        films: An array of struct with the following fields:
            film: The name of the film.
            votes: The number of votes the film received.
            rating: The rating of the film.
            filmid: A unique identifier for each film.

        quality_class: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
            star: Average rating > 8.
            good: Average rating > 7 and ≤ 8.
            average: Average rating > 6 and ≤ 7.
            bad: Average rating ≤ 6.

    is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
*/

-- DDL for films_type struct
DROP TYPE IF EXISTS films_type CASCADE
;

CREATE TYPE films_type AS (
                        film text
                        , year integer
                        , votes integer
                        , rating real
                        , filmid text)
;

DROP TYPE IF EXISTS quality_class_type CASCADE
;

CREATE TYPE quality_class_type AS
                        ENUM ('bad', 'average', 'good', 'star')
;

-- DDL for actors table
DROP TABLE IF EXISTS actors
;

CREATE TABLE actors (
            actor text
            , actorid text
            , year integer
            , films films_type[]
            , quality_class quality_class_type
            , is_active boolean
            , PRIMARY KEY (actor, actorid, year))
;