package com.edu.neu.csye7200.finalproject
import com.edu.neu.csye7200.finalproject.util.QueryUtil
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types._
import org.scalatest.tagobjects.Slow
import org.apache.spark.rdd.RDD
import com.edu.neu.csye7200.finalproject.Interface.MovieRecommendation
import com.edu.neu.csye7200.finalproject.util._
import scala.util.Random
class QuerySpec extends FlatSpec with Matchers with BeforeAndAfter {
  implicit var spark: SparkSession = _
  implicit var df: DataFrame = _
  implicit var keywords:RDD[(Int,String)]=_
  before {
    spark = SparkSession
      .builder()
      .appName("MovieRecommendation")
      .master("local[*]")
      .getOrCreate()

   df=DataUtil.getMoviesDF(getClass.getResource("movies_metadata.csv").getPath)

    keywords=DataUtil.getKeywords(getClass.getResource("keywords.csv").getPath)
//    df.persist()

  }

  behavior of "Spark Query "
  it should " work for query Drama type in genres" taggedAs Slow in{
    val content="Animation"
    Random.shuffle(MovieRecommendation.queryByGenres(content).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  it should "work for query Boy type in Keywords" taggedAs Slow in{
    val content="boy"
    Random.shuffle(MovieRecommendation.queryByKeywords(content).toSeq).take(5).filter(_._2.contains(content)).size should matchPattern{
      case 5=>
    }
  }
  after{
    spark.stop()
  }


}
