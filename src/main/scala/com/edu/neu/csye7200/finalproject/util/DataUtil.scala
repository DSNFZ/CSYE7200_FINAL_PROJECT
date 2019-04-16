package com.edu.neu.csye7200.finalproject.util

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{StructField, _}
import com.edu.neu.csye7200.finalproject.Schema._

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
  val schema =MovieSchema.movieSchema
  lazy val df=spark.read.option("header", true).schema(schema).csv("movies-dataset/movies_metadata.csv")

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
    * Get all the movie data of Array type
    * @param file   The path of the file
    * @return       Array of [[(Int, String)]] contiaining (movieId, title)
    */
  def getMoviesArray(file: String)  = {
    import spark.implicits._
    // There are some null id in movies data and filter them out
    df.select($"id", $"title").collect().filter(_(0) != null).map(x => (x.getInt(0), x.getString(1)))
  }

  /**
    * Get all the movie data of DataFrame type
    * @return       DataFrame contain all the information
    */
  def getMoviesDF = df
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
    val schema =MovieSchema.linkdataSchema


    val df = spark.read.option("header", true).schema(schema).csv(file)
    import spark.implicits._
    // Set tmdbId as the movie id and mapping to the id.
    df.select($"movieId", $"tmdbId").collect.filter(_(1) != null).map(x => (x.getInt(1), x.getInt(0))).toMap
  }

  /**
    * Get the keywords of movies which keywords formed in JSON format
    * @param file   The path of file
    * @return       DataFrame of keywords
    */
  def getKeywords(file: String) = {
    val schema =MovieSchema.keywordsSchema
    spark.read.option("header", true).schema(schema).csv(file)
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
