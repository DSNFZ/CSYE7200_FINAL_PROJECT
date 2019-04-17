package com.edu.neu.csye7200.finalproject

import com.edu.neu.csye7200.finalproject.util.DataUtil
import org.apache.spark.mllib.recommendation.Rating
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-16
  * Time: 17:54
  */
class DataSpec extends FlatSpec with Matchers {

  behavior of "getAllRating"
  it should "Read the header of the file and construct RDD" in {
    val test = getClass.getResource("test_rating.csv").getPath
    DataUtil.getAllRating(test).collect should matchPattern {
      case Array((9, Rating(1, 110, 1.0)), (5, Rating(1, 147, 4.5)), (9, Rating(2, 5, 3.0)))  =>
    }
  }

  behavior of "getRatingByUser"
  it should "Get rating record by userID which is set to 1" in {
    val test = getClass.getResource("test_rating.csv").getPath
    DataUtil.getRatingByUser(test,1).collect should matchPattern {
      case Array(Rating(1, 110, 1.0), Rating(1, 147, 4.5))  =>
    }
  }

  it should "Get rating record by userID which is set to 2" in {
    val test = getClass.getResource("test_rating.csv").getPath
    DataUtil.getRatingByUser(test,2).collect should matchPattern {
      case Array( Rating(2, 5, 3.0))  =>
    }
  }

  it should "Get empty array when userID does not exist in rating file" in {
    val test = getClass.getResource("test_rating.csv").getPath
    DataUtil.getRatingByUser(test,0).collect should matchPattern {
      case Array()  =>
    }
  }

  behavior of "getCandidatesAndLink"
  it should "Deal the link file and change the imdbID to movie id" in {
    val movies = Array((862, "the movie 1"), (900, "the movie 2"), (100, "the movie 3"))
    val links = Map((862, 1), (900, 2))
    DataUtil.getCandidatesAndLink(movies, links).toArray should matchPattern{
      case Array((1,"the movie 1"), (2,"the movie 2")) =>
    }
  }
}
