package com.edu.neu.csye7200.finalproject.util

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, _}

/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-02
  * Time: 14:41
  */
object DataUtil {

  lazy val spark = SparkSession
    .builder()
    .appName("MovieRecommondation")
    .master("local[*]")
    .getOrCreate()

  def getAllRating(file: String) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      // ( timestamp, user, product, rating)
      (fields(3).toLong%10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }

  def getMovies(file: String)  = {
    val schema = StructType(
      Seq(
        StructField("adult", BooleanType, true),
        StructField("belongs_to_collection", StringType, true),
        StructField("budget",IntegerType, true),
        StructField("genres",StringType, true),
        StructField("homepage",StringType, true),
        StructField("id",IntegerType, false),
        StructField("imdb_id",IntegerType, true),
        StructField("original_language",StringType, true),
        StructField("original_title",StringType, true),
        StructField("overview",StringType, true),
        StructField("popularity",FloatType, true),
        StructField("poster_path",StringType, true),
        StructField("production_companies",StringType, true),
        StructField("production_countries",StringType, true),
        StructField("release_data",DateType, true),
        StructField("revenue",IntegerType, true),
        StructField("runtime",FloatType, true),
        StructField("spoken_language",StringType, true),
        StructField("status",StringType, true),
        StructField("tagline",StringType, true),
        StructField("title",StringType, false),
        StructField("video",BooleanType, true),
        StructField("vote_average",FloatType, true),
        StructField("vote_count",IntegerType, true)
      )
    )

    val df = spark.read.option("header", true).schema(schema).csv(file)
    import spark.implicits._
    //There are some null id in movies data and filter them out
    df.select($"id", $"title").collect().filter(_(0) != null).map(x => (x.getInt(0), x.getString(1))).toMap
  }

  def getRatingByUser(file: String, userId: Int) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      (fields(3).toLong, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }.filter(row => userId == row._2.user)
      .map(_._2)
  }

}
