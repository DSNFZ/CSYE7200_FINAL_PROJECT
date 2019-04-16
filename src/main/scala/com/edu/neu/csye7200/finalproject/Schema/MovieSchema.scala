package com.edu.neu.csye7200.finalproject.Schema

import org.apache.spark.sql.types._

object MovieSchema {
  val movieSchema=StructType(
    Seq(
      StructField("adult", BooleanType, true),
      StructField("belongs_to_collection", StringType, true),
      StructField("budget",IntegerType, true),
      StructField("genres",StringType, true),
      StructField("homepage",StringType, true),
      StructField("id",IntegerType, true),
      StructField("imdb_id",IntegerType, true),
      StructField("original_language",StringType, true),
      StructField("original_title",StringType, true),
      StructField("overview",StringType, true),
      StructField("popularity",DoubleType, true),
      StructField("poster_path",StringType, true),
      StructField("production_companies",StringType, true),
      StructField("production_countries",StringType, true),
      StructField("release_date",DateType, true),
      StructField("revenue",IntegerType, true),
      StructField("runtime",FloatType, true),
      StructField("spoken_languages",StringType, true),
      StructField("status",StringType, true),
      StructField("tagline",StringType, true),
      StructField("title",StringType, true),
      StructField("video",BooleanType, true),
      StructField("vote_average",FloatType, true),
      StructField("vote_count",IntegerType, true)
    )
  )
  val linkdataSchema=StructType(
    Seq(
      StructField("movieId", IntegerType, false),
      StructField("imdbId", StringType, false),
      StructField("tmdbId", IntegerType, false)
    )
  )
  val keywordsSchema=StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("keywords", StringType, true)
    )
  )
}
