package com.edu.neu.csye7200.finalproject.Interface

import java.sql.Date
import com.github.tototoshi.csv._
import com.edu.neu.csye7200.finalproject.util.{ALSUtil, DataUtil, QueryUtil}

/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-09
  * Time: 16:20
  */
object MovieRecommendation {
  lazy val dir = "movies-dataset/"
  lazy val df=DataUtil.getMoviesDF

  def getRecommendation(userId: Int) = {
    //RDD[long, Rating]
    val ratings = DataUtil.getAllRating(dir+"ratings_small.csv")

    val moviesArray = DataUtil.getMoviesArray(dir + "movies_metadata.csv")

    val links = DataUtil.getLinkData(dir + "links_small.csv")
    val movies = DataUtil.getCandidatesAndLink(moviesArray, links)

    val userRatingRDD = DataUtil.getRatingByUser( dir+"ratings_small.csv",userId)
    val userRatingMovie = userRatingRDD.map(x => x.product).collect

    val numRatings = ratings.count()
    val numUser = ratings.map(_._2.user).distinct().count()
    val numMovie = ratings.map(_._2.product).distinct().count()

    println("rating: " + numRatings + " movies: " + numMovie
      + " user: " + numUser)

    // Split data into train(60%), validation(20%) and test(20%)
    val numPartitions = 10

    val trainSet = ratings.filter(x => x._1 < 6).map(_._2).
      union(userRatingRDD).repartition(numPartitions).persist()
    val validationSet = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .map(_._2).persist()
    val testSet = ratings.filter(x => x._1 >= 8).map(_._2).persist()

    val numTrain = trainSet.count()
    val numValidation = validationSet.count()
    val numTest = testSet.count()

    println("Training data: " + numTrain + " Validation data: " + numValidation
      + " Test data: " + numTest)

    //      Train model and optimize model with validation set
    ALSUtil.trainAndRecommendation(trainSet, validationSet, testSet, movies, userRatingRDD)
  }

  def queryBySelectedInMoviesJson(content: String,SelectedType:String) = {
    QueryUtil.QueryMovie(df,content,SelectedType)
  }
  def queryBySeletedInMoviesNormal(content: String,SelectedType:String)={
    QueryUtil.QueryMovieInfo(df,content,SelectedType)
  }
  def queryByKeywords(content: String) = {
    //Query of keywords
    val keywordsRDD = DataUtil.getKeywords(dir + "keywords.csv")
    QueryUtil.QueryOfKeywords(keywordsRDD, df,content)
  }
  def SortBySelected(ds:Array[(Int,String,String,String,Date,Double)],selectedType:String="popularity",order:String="desc")={
    selectedType match{
      case "popularity"=> order match{
        case "desc"=> ds.sortBy(-_._6)
        case "asc"=>ds.sortBy(_._6)
      }
      case "release_date"=>
        order match{
          case "desc"=> ds.sortWith(_._5.getTime>_._5.getTime)
          case _=>ds.sortBy(-_._5.getTime)
        }
    }
  }
  def queryBystaffInCredits(content: String,SelectedType:String)={
    QueryUtil.QueryOfstaff(DataUtil.getStaff(dir+"credits.csv"),df,content,SelectedType)
  }
  def UpdateRatingsByRecommendation[T](RatingsInfo:List[T])={
    val  writer=CSVWriter.open(dir+"ratings_small.csv",append=true)
    writer.writeRow(RatingsInfo)
    writer.close()
  }

}

