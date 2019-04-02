/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-02
  * Time: 15:13
  */

import com.edu.neu.csye7200.finalproject.util.{ALSUtil, DataUtil}
object Main extends App {
  override def main(args: Array[String]): Unit = {
    if(args.length > 0){
      //load the data and get the rating data of specific user
      val dir = args.head
      val ratings = DataUtil.getAllRating(dir + "ratings.csv")
      val movies = DataUtil.getMovies(dir + "movies_metadata.csv")

      val userRatingRDD = DataUtil.getRatingByUser(dir + "ratings.csv", 1)

      ratings.take(10).foreach(println)
      movies.take(10).foreach(println)
      userRatingRDD.foreach(println)

      val numPartitions = 10

      val trainSet = ratings.filter(x => x._1 < 6).map(_._2).
        union(userRatingRDD).repartition(numPartitions).persist()
      val validationSet = ratings.filter(x => x._1 >= 6 && x._1 < 8)
        .map(_._2).persist()
      val testSet = ratings.filter(x => x._1 >= 8).map(_._2).persist()

      val numRatings = ratings.count()
      val numUser = ratings.map(_._2.user).distinct().count()
      val numMovie = ratings.map(_._2.product).distinct().count()

      println("rating: " + numRatings + " movies: " + numMovie
        + " user: " + numUser)

      val numTrain = trainSet.count()
      val numValidation = validationSet.count()
      val numTest = testSet.count()

      println("Training data: " + numTrain + " Validation data: " + numValidation
        + " Test data: " + numTest)

      //Train model and optimize model with validation set
      ALSUtil.trainAndOptimizeModel(trainSet, validationSet)
      ALSUtil.evaluateMode(trainSet, validationSet, testSet)
      val recommendations = ALSUtil.makeRecommendation(movies,userRatingRDD)
      var i = 0
      println("Movies recommended for you:")
      recommendations.foreach{ line=>
        println("%2d".format(i)+" :"+movies(line.product))
        i += 1
      }

    }
    DataUtil.spark.stop()
  }
}
