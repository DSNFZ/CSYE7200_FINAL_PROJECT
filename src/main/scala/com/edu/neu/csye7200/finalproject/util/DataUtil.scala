package com.edu.neu.csye7200.finalproject.util

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.{StructField, _}

/**
  * The Util object for file reading and data extraction
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

  /**
    * Get RDD object from ratings.csv file which contains all the rating information
    * @param file   The path of the file
    * @return       RDD of [[(Long, Rating)]] with(timestamp % 10, user, product, rating)
    */
  def getAllRating(file: String) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      // (timestamp, user, product, rating)
      (fields(3).toLong%10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }

  /**
    * Get all the movie data
    * @param file   The path of the file
    * @return       Array of [[(Int, String)]] contiaining (movieId, title)
    */
  def getMovies(file: String)  = {
    val schema = StructType(
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
        StructField("popularity",FloatType, true),
        StructField("poster_path",StringType, true),
        StructField("production_companies",StringType, true),
        StructField("production_countries",StringType, true),
        StructField("release_date",DateType, true),
        StructField("revenue",IntegerType, true),
        StructField("runtime",FloatType, true),
        StructField("spoken_language",StringType, true),
        StructField("status",StringType, true),
        StructField("tagline",StringType, true),
        StructField("title",StringType, true),
        StructField("video",BooleanType, true),
        StructField("vote_average",FloatType, true),
        StructField("vote_count",IntegerType, true)
      )
    )
    val df = spark.read.option("header", true).schema(schema).csv(file)
    import spark.implicits._
    // There are some null id in movies data and filter them out
    df.select($"id", $"title").collect().filter(_(0) != null).map(x => (x.getInt(0), x.getString(1)))
  }
  /**
    * Get the rating information of specific user
    * @param file   The path of the file
    * @param userId user Id
    * @return       RDD of[[Rating]] with (user, product, rating)
    */
  def getRatingByUser(file: String, userId: Int) = {
    var rating = spark.read.textFile(file)
    val header = rating.first()
    rating = rating.filter(row => row != header)
    rating.rdd.map { line =>
      val fields = line.split(",")
      // (timestamp, user, product, rating)
      (fields(3).toLong, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }.filter(row => userId == row._2.user)
      .map(_._2)

  }

  /**
    * Get the movieId and tmdbId
    * @param file   The path of file
    * @return       Map of [[Int, Int]] with (id and tmdbId)
    */
  def getLinkData(file: String) = {
    val schema = StructType(
      Seq(
        StructField("movieId", IntegerType, false),
        StructField("imdbId", StringType, false),
        StructField("tmdbId", IntegerType, false)
      )
    )


    val df = spark.read.option("header", true).schema(schema).csv(file)
    import spark.implicits._
    // Set tmdbId as the movie id and mapping to the id.
    df.select($"movieId", $"tmdbId").collect.filter(_(1) != null).map(x => (x.getInt(1), x.getInt(0))).toMap
  }

  /**
    * Get the Candidate movies and replace the id with tmdbId
    * @param movies   Array of [[Int, String]] with (Id, title)
    * @param links   Map of [[Int, Int]] with (movieId, imdbId)
    * @return         Map of [[Int, String]]
    */
  def getCandidatesAndLink(movies: Array[(Int, String)], links: Map[Int, Int]) = {
    movies.filter(x => !links.get(x._1).isEmpty).map(x => (links(x._1), x._2)).toMap
  }

}
