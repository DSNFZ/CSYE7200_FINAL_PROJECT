package com.edu.neu.csye7200.finalproject.util

import com.fasterxml.jackson.core.JsonParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods.{compact, mapper, parse}
import com.redis._
import scala.util.Random
object QueryUtil {

  /** query movieid by information and information type
    * @param content user's input content
    * @param selectedType user's select for type of content ( companies,keywords,names)
    * @return  Iterable[Int] set of target movie id
    */
  def QueryMovie(df:DataFrame,content:String,selectedType:String) ={

    val colsList = List(col("id"),col(selectedType))
   df.select(colsList:_*).filter(_(0)!=null).rdd
      .map(row=>(row.getInt(0),parse(row.getString(1)
        .replaceAll("'","\""))))
      .map(x=>(x._1,compact(x._2\"name")))
      .filter(x=>x._2.contains(content)).collect
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


  def QueryOfKeywords[T](keywords: RDD[(Int,String)],df:DataFrame, content: String) = {
//    mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true)

   val ids= keywords
     .map(row => (row._1, parse(row._2.replaceAll("'", "\""))))
      .map(x=>(x._1,compact(x._2\"name")))
      .filter(x => x._2.contains(content)).collect.take(20)
//    print("SIZE",ids.size)
    ids.flatMap(id=>df.select("title","tagline","release_date","popularity").where("id=="+id._1).rdd.map{
        line =>(id._1,id._2,line.getString(0),line.getString(1),line.getDate(2),line.getFloat(3))
      }.collect)

  }
  def SortResult[T](result:Seq[T],OrderType:String)={}
}
