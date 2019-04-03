package com.edu.neu.csye7200.finalproject.util

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-02
  * Time: 17:38
  */
object ALSUtil {
  val numRanks = List(8, 12, 20)
  val numIters = List(10, 15, 20)
  val numLambdas = List(0.1, 10.0)
  var bestRmse = Double.MaxValue
  var bestModel: Option[MatrixFactorizationModel] = None
  var bestRanks = -1
  var bestIters = 0
  var bestLambdas = -1.0

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val prediction = model.predict(data.map(x=>(x.user, x.product)))
    val predDataJoined = prediction.map(x=> ((x.user,x.product),x.rating))
      .join(data.map(x=> ((x.user,x.product),x.rating))).values
    new RegressionMetrics(predDataJoined).rootMeanSquaredError
  }

  def trainAndOptimizeModel(trainSet: RDD[Rating], validationSet: RDD[Rating]): Unit ={
    for(rank <- numRanks; iter <- numIters; lambda <- numLambdas){
      val model = ALS.train(trainSet, rank, iter, lambda)
      val validationRmse = computeRmse(model, validationSet)

      println("RMSE(validation) = " + validationRmse + "with rank = " + rank
        + ", iter = " + iter + ", lambda = " + lambda)

      if (validationRmse < bestRmse) {
        bestModel = Some(model)
        bestRmse = validationRmse
        bestIters = iter
        bestLambdas = lambda
        bestRanks = rank
      }
    }
  }

  def evaluateMode(trainSet: RDD[Rating], validationSet: RDD[Rating], testSet: RDD[Rating]) = {
    val testRmse = computeRmse(bestModel.get, testSet)
    println("The best model was trained with rank=" + bestRanks + ", Iter=" + bestIters
      + ", Lambda=" + bestLambdas + " and compute RMSE on test is " + testRmse)

    val meanRating = trainSet.union(validationSet).map(_.rating).mean()

    val bestlineRmse = new RegressionMetrics(testSet.map(x => (x.rating, meanRating)))
      .rootMeanSquaredError
    val improvement = (bestlineRmse - testRmse) / bestlineRmse * 100
    println("The best model improves the baseline by "+"%1.2f".format(improvement)+"%.")
  }

  def makeRecommendation(movies: Map[Int, String],userRating: RDD[Rating]) = {

    val movieId = userRating.map(_.product).collect.toSeq
    movieId.foreach(println)
    val candidates = DataUtil.spark.sparkContext.parallelize(movies.keys.filter(!movieId.contains(_)).toSeq)
    userRating.foreach(println)
    movieId.foreach(println)
    bestModel.get
      .predict(candidates.map(x=>(1,x)))
      .sortBy(-_.rating)
      .take(20)
  }

  def trainAndRecommendation(trainSet: RDD[Rating], validationSet: RDD[Rating], testSet: RDD[Rating]
                             , movies: Map[Int, String],userRating: RDD[Rating]) ={
    trainAndOptimizeModel(trainSet, validationSet)
    evaluateMode(trainSet, validationSet, testSet)
    val recommendations = makeRecommendation(movies, userRating)
    var i = 1
    println( "Movies recommended for you:")
    recommendations.foreach{ line=>
      println("%2d".format(i)+" :"+movies(line.product))
      i += 1
    }
  }
}
