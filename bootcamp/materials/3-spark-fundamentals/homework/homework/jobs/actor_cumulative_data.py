from pyspark.sql import SparkSession

def do_actors_cumulative_transformation(spark, d_actors, d_actor_films, year, year1):
    query = f"""
        WITH last_year AS (
            SELECT actor, actorid, year, films, quality_class, is_active
            FROM actors
            WHERE year = '{year1}'       
        ), this_year AS (
            SELECT actor, actorid, year
            , array_agg(struct(
                         film
                        , year
                        , votes
                        , rating
                        , filmid)) AS films
            , ROUND(AVG(rating)) AS avg_rating
            FROM actor_films
            WHERE year = '{year}'
            GROUP BY actor, actorid, year
        )
        SELECT
            COALESCE(ls.actor, ts.actor) as actor
            , COALESCE(ls.actorid, ts.actorid) as actorid
            , COALESCE(ts.year, ls.year + 1) as year
            , case when ts.year is null then ls.films 
            when ls.year is not null then concat(ls.films, ts.films)
            when ls.year is null then ts.films 
            end AS films
            , CASE WHEN ts.year IS NOT NULL THEN
                    (CASE WHEN avg_rating > 8 THEN 'star'
                    WHEN avg_rating BETWEEN 7 AND 8 THEN 'good'
                    WHEN avg_rating BETWEEN 6 AND 7 THEN 'average'
                    ELSE 'bad' END)
                ELSE ls.quality_class
                END AS quality_class
            , ts.year IS NOT NULL AS is_active
            FROM last_year ls
            FULL OUTER JOIN this_year ts
            ON ls.actorid = ts.actorid
        ;    
    """
    d_actors.createOrReplaceTempView("actors")
    d_actor_films.createOrReplaceTempView("actor_films")
    return spark.sql(query)


def main():
    year = '1972'
    year1 = '1971'
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_cumulative") \
      .getOrCreate()
    
    output_df = do_actors_cumulative_transformation(spark, spark.table("bootcamp.actors"), spark.table("bootcamp.actor_films"), year, year1)
    #output_df.show()
    output_df.write.mode("append").insertInto("bootcamp.actors")