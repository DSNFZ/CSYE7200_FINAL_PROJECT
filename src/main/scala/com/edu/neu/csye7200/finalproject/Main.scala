package com.edu.neu.csye7200.finalproject

import com.edu.neu.csye7200.finalproject.util.{ALSUtil, DataUtil,QueryUtil}
import com.edu.neu.csye7200.finalproject.Interface.MovieRecommendation._

/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-02
  * Time: 15:13
  */
object Main extends App {
  override def main(args: Array[String]): Unit = {
//    getRecommendation(1)
//   print( queryByGenres("Animation").take(4).filter(_._2.contains("Animation")).size)
//   queryByKeywords("boy").map(x=>.foreach(line=>println(line._6)))
//    queryByKeywords("boy").sortWith(_._6>_._6).foreach(line=>println("id: "+line._1,"popularity: "+
//      line._6,"title: "+line._3,"keywords: "+line._2,"tagline: "+line._4,"release_date: "+line._5))
    queryByKeywords("boy").take(5).sortBy(-_._5.getTime()).foreach(line=>println("id: "+line._1,"popularity: "+
      line._6,"title: "+line._3,"keywords: "+line._2,"tagline: "+line._4,"release_date: "+line._5))

    DataUtil.spark.stop()
  }
}
