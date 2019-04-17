package com.edu.neu.csye7200.finalproject

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.tagobjects.Slow
import com.edu.neu.csye7200.finalproject.Interface.MovieRecommendation
import com.edu.neu.csye7200.finalproject.util._
import scala.util.Random
class QuerySpec extends FlatSpec with Matchers with BeforeAndAfter {

  before {
  val df = DataUtil.getMoviesDF
  }
  behavior of "Spark Query "
  it should " work for query Drama type in genres" taggedAs Slow in{
    val content="Animation"
    val SelectedType="genres"
    Random.shuffle(MovieRecommendation.queryBySelectedInMoviesJson(content,SelectedType).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  it should "work for query Boy type in Keywords" taggedAs Slow in{
    val content="boy"

    Random.shuffle(MovieRecommendation.queryByKeywords(content).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }

  it should "work for query  United States of America  in production_countries " taggedAs Slow in{
    val content="America"
    val SelectedType="production_countries"
    Random.shuffle(MovieRecommendation.queryBySelectedInMoviesJson(content,SelectedType).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  it should "work for query  Pixar Animation Studios  in production_companies " taggedAs Slow in{
    val content="Pixar Animation Studios"
    val SelectedType="production_companies"
    Random.shuffle(MovieRecommendation.queryBySelectedInMoviesJson(content,SelectedType).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  it should "work for query  English  in spoken_language " taggedAs Slow in{
    val content="English"
    val SelectedType="spoken_languages"
    Random.shuffle(MovieRecommendation.queryBySelectedInMoviesJson(content,SelectedType).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  it should "work for query  Andy Tennant(director)  in crew " taggedAs Slow in{
    val content="Andy Tennant"
    Random.shuffle(MovieRecommendation.queryBystaffInCredits(content,"crew").toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  it should "work for query  Tom Hanks  in cast " taggedAs Slow in{
    val content="Tom Hanks"
    Random.shuffle(MovieRecommendation.queryBystaffInCredits(content,"cast").toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  it should "work for query title which contains a in title " taggedAs Slow in{
    val content="a"
    Random.shuffle(MovieRecommendation.queryBySeletedInMoviesNormal(content,"title").toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }



}
