package com.edu.neu.csye7200.finalproject
import com.edu.neu.csye7200.finalproject.util.QueryUtil
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types._
import org.scalatest.tagobjects.Slow

class QuerySpec extends FlatSpec with Matchers with BeforeAndAfter {
  implicit var spark: SparkSession = _
  implicit var df: DataFrame = _
  before {
    spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()
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
        StructField("spoken_languages",StringType, true),
        StructField("status",StringType, true),
        StructField("tagline",StringType, true),
        StructField("title",StringType, true),
        StructField("video",BooleanType, true),
        StructField("vote_average",FloatType, true),
        StructField("vote_count",IntegerType, true)
      )
    )

     df=spark.read.format("csv").option("header", "true").schema(schema).load(getClass.getResource("movies_metadata.csv").getPath)
    df.persist()

  }

  after {
    if (spark != null) {
      df.unpersist()
      spark.stop()
    }
  }

  behavior of "Spark Query"
  it should "work for query Drama type" taggedAs Slow in{
    print(getClass.getResource("movies_metadata.csv").getPath)
  // val df= QueryUtil.GetMovies(getClass.getResource("movies_metadata.csv").getPath)
    QueryUtil.QueryMovie(df,"Drama","genres").take(5).filter(_._2.contains("Drama")).size should matchPattern{
      case 5=>
    }
  }
}
