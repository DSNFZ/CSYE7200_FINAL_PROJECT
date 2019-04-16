package com.edu.neu.csye7200.finalproject.util
import scala.util.{Try,Success,Failure}
import com.fasterxml.jackson.core.JsonParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, mapper, parse}

import scala.util.{Random, Try}
object QueryUtil {

  /** query movieid by information and information type
    *
    * @param content      user's input content
    * @param selectedType user's select for type of content ( companies,keywords,names)
    * @return Iterable[Int] set of target movie id
    */
  def QueryMovie(df: DataFrame, content: String, selectedType: String) = {

    val colsList = List(col("id"), col(selectedType))
    DataClean(df.select(colsList: _*))
      .map(x => (x._1, compact(x._2 \ "name")))
      .filter(x => x._2.contains(content)).collect
    //mapValues(x=>x.filter(row=>compact(render(row))==compact(content))).filter(_._2.nonEmpty).keys
  }
  def DataClean(df:DataFrame)={
     val invalidSeq=Seq(("'"),("'name'"))
    df.rdd.filter(_(0)!= null).filter(_(1)!=null).filter(x=> (x.getString(1).contains("'"))).filter(x=> (x.getString(1).contains("'name'")))
      .filter(row=> !row.getString(1).takeRight(1).equals("'"))
      .map(row=>(row.getInt(0),parse(row.getString(1).replaceAll("'","\"").replaceAll("\\\\xa0","")
        .replaceAll("\\\\",""))))
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
  /**
    * Query movieid by keywords
    *
    * @param keywords Keywords data of each movie
    * @param content  User's input content
    * @return Map[Int, String] with (id, keywords)
    */

  def QueryOfKeywords[T](keywords:DataFrame, df: DataFrame, content: String) = {
    //    mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true)

    val ids = DataClean(keywords)
      .map(x => (x._1, compact(x._2 \ "name")))
      .filter(x => x._2.contains(content)).collect.take(20)
    //    print("SIZE",ids.size)
    ids.flatMap(id => df.select("title", "tagline", "release_date", "popularity").where("id==" + id._1).rdd.map {
      line => (id._1, id._2, line.getString(0), line.getString(1), line.getDate(2), line.getFloat(3))
    }.collect)


  }

}
