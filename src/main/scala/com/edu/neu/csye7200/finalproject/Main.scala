package com.edu.neu.csye7200.finalproject

import com.edu.neu.csye7200.finalproject.util.{ALSUtil, DataUtil,QueryUtil}
import com.edu.neu.csye7200.finalproject.Interface.MovieRecommendation._
//import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Props }
/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-02
  * Time: 15:13
  */

object Main extends App {
  override def main(args: Array[String]): Unit = {
        for(e <- List(1,2)) yield getRecommendation(e)
//        queryByGenres("Animation").take(5).foreach(x=>println(x._1,x._2))
//
//        queryByKeywords("boy").sortWith(_._6>_._6).take(5).foreach(line=>println("id: "+line._1,"popularity: "+
//          line._6,"title: "+line._3,"keywords: "+line._2,"tagline: "+line._4,"release_date: "+line._5))
//        for(x<-List("boy") )yield queryByKeywords("boy").take(5).sortBy(_._5.getTime()).foreach(line=>println("id: "+line._1,
//          "popularity: "+ line._6,"title: "+line._3,"keywords: "+line._2,"tagline: "+line._4,"release_date: "+line._5))
    SortBySelected(queryBySpokenLanguages("English").take(5),order="desc").foreach(x=>println("id"+x._1,"spoken_languages"+x._2,x._6))
    SortBySelected(queryBySpokenLanguages("English").take(5),order="asc").foreach(x=>println("id"+x._1,"spoken_languages"+x._2,x._6))
        DataUtil.spark.stop()

  }
}