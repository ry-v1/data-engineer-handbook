from chispa.dataframe_comparer import *
from ..jobs.actor_cumulative_data import do_actors_cumulative_transformation
from collections import namedtuple
actor_films = namedtuple("actor_films", "actor, actorid, film , year , votes , rating , filmid")
films = namedtuple("films", "film, year1, votes, rating, filmid")
actors = namedtuple("actors", "actor, actorid, year, films, quality_class , is_active")
#schema =   [(actor STRING), (actorid STRING), (year INT), (films array<struct<film STRING, year int, votes int, rating float, filmid STRING>>), (quality_class STRING), (is_active BOOL)]
from pyspark.sql.types import *

schema = StructType([
  StructField("actor", StringType())
  , StructField("actorid", StringType())
  , StructField("year", LongType())
  , StructField("films", ArrayType(
      StructType([
          StructField("film", StringType()),
          StructField("year", LongType()),
          StructField("votes", LongType()),
          StructField("rating", DoubleType()),
          StructField("filmid", StringType())
      ])
   )
  )
   , StructField("quality_class", StringType())
   , StructField("is_active", BooleanType(), False)
])

def test_scd_generation(spark):
    source_data = [
        actor_films("Tom Hanks", "nm0000158", "Bachelor Party", 1984, 35846, 6.3, "tt0086927"),
        actor_films("Tom Hanks", "nm0000158", "Volunteers", 1984, 8720, 5.6, "tt0090274"),
        actor_films("Tom Hanks", "nm0000158", "Big", 1986, 8720, 5.6, "tt0090274"),
    ]
    data =[actors("Tom Hanks", "nm0000158", 1984, [("Bachelor Party", 1984, 35846, 6.3, "tt0086927"), ("Volunteers", 1984, 8720, 5.6, "tt0090274")], "average", True),
        actors("Tom Hanks", "nm0000158", 1985, [("Bachelor Party", 1984, 35846, 6.3, "tt0086927"), ("Volunteers", 1984, 8720, 5.6, "tt0090274")], "average", False),
    ]

    source_df = spark.createDataFrame(source_data)
    actor_df = spark.createDataFrame(data=data, schema=schema)

    actual_df = do_actors_cumulative_transformation(spark, actor_df, source_df, 1986, 1985)
    expected_data = [
         actors("Tom Hanks", "nm0000158", 1986, [("Bachelor Party", 1984, 35846, 6.3, "tt0086927"), ("Volunteers", 1984, 8720, 5.6, "tt0090274"), ("Big", 1986, 8720, 5.6, "tt0090274")], "average", True)
    ]
    expected_df = spark.createDataFrame(expected_data,schema=schema)
    assert_df_equality(actual_df, expected_df)

    source_data = [
        actor_films("Tom Hanks", "nm0000158", "Bachelor Party", 1984, 35846, 6.3, "tt0086927"),
        actor_films("Tom Hanks", "nm0000158", "Volunteers", 1984, 8720, 5.6, "tt0090274"),
    ]
    data =[]

    source_df = spark.createDataFrame(source_data)
    actor_df = spark.createDataFrame(data=data, schema=schema)

    actual_df = do_actors_cumulative_transformation(spark, actor_df, source_df, 1984, 1983)
    expected_data = [
         actors("Tom Hanks", "nm0000158", 1984, [("Bachelor Party", 1984, 35846, 6.3, "tt0086927"), ("Volunteers", 1984, 8720, 5.6, "tt0090274")], "average", True)
    ]
    expected_df = spark.createDataFrame(expected_data,schema=schema)
    assert_df_equality(actual_df, expected_df)