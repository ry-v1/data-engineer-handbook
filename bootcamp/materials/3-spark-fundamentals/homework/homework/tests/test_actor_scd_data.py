from chispa.dataframe_comparer import *
from ..jobs.actor_scd_data import do_actors_scd_transformation
from collections import namedtuple

actor_films = namedtuple("actor_films", "actor, actorid, film , year , votes , rating , filmid")
films = namedtuple("films", "film, year1, votes, rating, filmid")
actors = namedtuple("actors", "actor, actorid, year, quality_class , is_active")
actors_scd = namedtuple("actors_scd", "actor, quality_class , is_active, start_date, end_date")

from pyspark.sql.types import *

schema = StructType([
  StructField("actor", StringType())
   , StructField("actorid", StringType())
   , StructField("year", LongType())
   , StructField("quality_class", StringType())
   , StructField("is_active", BooleanType())
])


schemaSCD = StructType([
  StructField("actor", StringType())
   , StructField("quality_class", StringType())
   , StructField("is_active", BooleanType())
   , StructField("start_date", LongType())
   , StructField("end_date", LongType())
])

def test_scd_generation(spark):
    source_data = [
        actors("Tom Hanks", "nm0000158", 1984, "average", True),
        actors("Tom Hanks", "nm0000158", 1985, "average", False),
        actors("Tom Hanks", "nm0000158", 1986, "average", True)
    ]
    source_df = spark.createDataFrame(source_data, schema=schema)

    actual_df = do_actors_scd_transformation(spark, source_df)
    expected_data = [
        actors_scd("Tom Hanks", "average", True, 1984, 1984),
        actors_scd("Tom Hanks", "average", False, 1985, 1985),
        actors_scd("Tom Hanks", "average", True, 1986, 1986)
    ]
    expected_df = spark.createDataFrame(expected_data,schema=schemaSCD)
    assert_df_equality(actual_df, expected_df)