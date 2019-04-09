package com.edu.neu.csye7200.finalproject
import com.edu.neu.csye7200.finalproject.util.QueryUtil
import org.scalatest.{FlatSpec,Matchers,BeforeAndAfter}
import org.apache.spark.sql.{SparkSession}
import org.scalatest.tagobjects.Slow

class QuerySpec extends FlatSpec with Matchers with BeforeAndAfter {
  implicit var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }
  behavior of "Spark Query"
  it should "work for query Drama type" taggedAs Slow in{
    print(getClass.getResource("movies_metadata.csv").getPath)
   val df= QueryUtil.GetMovies(getClass.getResource("movies_metadata.csv").getPath)
    QueryUtil.QueryMovie(df,"Drama","genres").take(5).filter(_._2.contains("Drama")).size should matchPattern{
      case 5=>
    }
  }
}
