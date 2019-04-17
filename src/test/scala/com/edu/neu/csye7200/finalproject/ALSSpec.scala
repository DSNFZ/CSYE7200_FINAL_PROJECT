package com.edu.neu.csye7200.finalproject

import com.edu.neu.csye7200.finalproject.Interface.MovieRecommendation
import org.scalatest.tagobjects._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random
/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-16
  * Time: 15:24
  */
class ALSSpec extends FlatSpec with Matchers {

  behavior of "Spark Recommendation"
  it should "the RMSE of Users should < 1" taggedAs Slow in{
    val e = List(1,2,3,4,5)
    val ids=e.map(x => Random.nextInt(550))
    ids.flatMap(MovieRecommendation.getRecommendation)
      .count(x=>x<1.0) should matchPattern{
      case 5 =>
    }
  }
}
