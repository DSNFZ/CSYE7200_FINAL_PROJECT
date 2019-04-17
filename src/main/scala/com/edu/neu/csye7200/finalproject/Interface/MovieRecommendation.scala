package com.edu.neu.csye7200.finalproject.Interface

import java.sql.Date

import com.edu.neu.csye7200.finalproject.configure.FileConfig
import com.edu.neu.csye7200.finalproject.util.{ALSUtil, DataUtil, QueryUtil}

/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-09
  * Time: 16:20
  */
object MovieRecommendation {

  lazy val df=DataUtil.getMoviesDF

  /**
    * This function trained the data and get the recommendation movie
    * for specific user and print the result.
    * @param userId     The specific user of recommendation
    * @return           Return the RMSE and improvement of the Model
    */
  def getRecommendation(userId: Int) = {
    //RDD[long, Rating]
    val ratings = DataUtil.getAllRating(FileConfig.ratingFile)

    val moviesArray = DataUtil.getMoviesArray

    val links = DataUtil.getLinkData(FileConfig.linkFile)
    val movies = DataUtil.getCandidatesAndLink(moviesArray, links)

    val userRatingRDD = DataUtil.getRatingByUser(FileConfig.ratingFile, userId)
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

  /**
    * Search movie by specific genres
    * @param content  The specified genres
    * @return         Array of [[Int, String, String, String, Date, Double]]
    *                 with (movidId,selectedType,title,tagline,release_date,popularity)
    */
  def queryByGenres(content: String) = {
    QueryUtil.QueryMovie(df,content,"genres")
  }

  /**
    * Search movie by specific Countries
    * @param content  The specified countries
    * @return         Array of Array[(Int,String,String,String,Date,Double)]
    *                 with (movidId,selectedType,title,tagline,release_date,popularity)
    */
  def queryByCountries(content: String) = {
    QueryUtil.QueryMovie(df,content,"production_countries")
  }

  /**
    * Search movie by production companies
    * @param content  The specified companies
    * @return         Array of [(Int,String,String,String,Date,Double)]
    *                 with (movidId,selectedType,title,tagline,release_date,popularity)
    */
  def queryByProductionCompanies(content: String) = {
    QueryUtil.QueryMovie(df,content,"production_companies")
  }

  /**
    * Search movie by spoken Languages
    * @param content  The specified language
    * @return         Array of [(Int,String,String,String,Date,Double)]
    *                 with (movidId,selectedType,title,tagline,release_date,popularity)
    */
  def queryBySpokenLanguages(content: String) = {
    QueryUtil.QueryMovie(df,content,"spoken_languages")
  }

  /**
    * Search movie by keywords
    * @param content  The specified keywords
    * @return         Array of [(Int,String,String,String,Date,Double)]
    *                 with (movidId,selectedType,title,tagline,release_date,popularity)
    */
  def queryByKeywords(content: String) = {
    //Query of keywords
    val keywordsRDD = DataUtil.getKeywords(FileConfig.keywordsFile)
    QueryUtil.QueryOfKeywords(keywordsRDD, df,content)
  }

  /**
    * Sort the Array of movies
    * @param ds             The dataset of movies to be sorted
    * @param selectedType   The sort key word
    * @param order          The order type: desc or asc
    * @return               Sorted movie dataset
    */
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

  /**
    * Search movie by staffs
    * @param content        The user input of specific staff
    * @param SelectedType   Specify the content type: crew or cast
    * @return               Array of [[Int, String, String, String, Date, Double]]
    *                       with (id, staff,title,tagline,release_date,popularity)
    */
  def queryBystaff(content: String,SelectedType:String)={
    QueryUtil.QueryOfstaff(DataUtil.getStaff(FileConfig.creditFIle),df,content,SelectedType)
  }

}
