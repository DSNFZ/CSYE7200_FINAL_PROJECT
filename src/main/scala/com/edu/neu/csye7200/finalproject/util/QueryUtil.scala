package com.edu.neu.csye7200.finalproject.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods.{compact, parse}

object QueryUtil {
  lazy val spark = SparkSession
    .builder()
    .appName("MovieRecommondation")
    .master("local[*]")
    .getOrCreate()
  /** extract all movie information
    *
    */
  def GetMovies(File:String)={
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

    spark.read.option("header", true).schema(schema).csv(File)
    //    import spark.implicits._
    //
    //
    //    df.select("*")
    //    val content="Drama"
    //r.map(x=>x.filter(row=>compact(render(row))==compact(content)))
  }
  /** query movieid by information and information type
    * @param content user's input content
    * @param selectedType user's select for type of content ( companies,keywords,names)
    * @return  Iterable[Int] set of target movie id
    */
  def QueryMovie(df:DataFrame,content:String,selectedType:String) ={

    val colsList = List(col("id"),col(selectedType))
    df.select(colsList:_*).filter(_(0)!=null).rdd.collect
      .map(row=>(row.getInt(0),parse(row.getString(1)
        .replaceAll("'","\"")))).toMap.mapValues(r=>(r\"name"))
      .filter(x=>compact(x._2).contains(content)).map(x=>(x._1,compact(x._2)))
    //mapValues(x=>x.filter(row=>compact(render(row))==compact(content))).filter(_._2.nonEmpty).keys
  }
//  def QueryMovieInfo(df:DataFrame,ids:Iterable[Int])={
//    val Infos =ids.map(id=>df.select("id","genres").where("id=="+compact(id)).collect)
//    Infos.map(row=>(row(0).getInt(0),parse(row(0).getString(1).replaceAll("'","\"")))).toMap.mapValues(r=>compact(r\"name"))
//  }
  //  def ExtractInfoFromJsonLike(rows:  Iterable[Array[org.apache.spark.sql.Row]])={
  //    rows.map(row=>(row(0).getInt(0),parse(row(0).getString(1).replaceAll("'","\"")))).toMap.mapValues(r=>compact(r\"name"))
  ////  }
  //  def ExtractInfoFromJsonLike(rows:Array[org.apache.spark.sql.Row])={
  //    rows.map(row=>(row.getInt(0),parse(row.getString(1).replaceAll("'","\"")))).toMap.mapValues(r=>(r\"name"))
  //  }
}
