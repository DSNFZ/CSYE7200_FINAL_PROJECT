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
    getRecommendation(1)
    queryByGenres("Animation")
    queryByKeywords("boy")
    DataUtil.spark.stop()
  }
}
