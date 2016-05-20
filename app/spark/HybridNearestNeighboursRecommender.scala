package spark

import javax.inject.Inject

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import play.api.Configuration

import scala.collection.mutable

/**
  * Created by P. Akhmedzianov on 17.04.2016.
  */
class HybridNearestNeighboursRecommender @Inject()(configuration: Configuration)
  extends MlLibAlsSparkRatingsRecommender(configuration) with java.io.Serializable {

  val numbersOfNeighboursList = List(50)

  private var numberOfNeighboursToFind_ = 40

  protected var meansAndDeviationsRddOption_ : Option[RDD[(Int, (Double, Double))]] = None
  protected var meansRddOption_ : Option[RDD[(Int, Double)]] = None
  private var neighboursRddOption_ : Option[RDD[(Int, Array[(Int, Double)])]] = None


  override def test(): Unit ={
    println("Cosine similarity:")
    testAverages(cosineSimilarity)
    println("Pearson correlation:")
    testAverages(pearsonCorrelation)
  }

  private def testAverages(similarityMeasure: (Array[Double], Array[Double]) => Option[Double] ): Unit = {
    val arrayBufferRmse = mutable.ArrayBuffer[Double]()
    val arrayBufferMae = mutable.ArrayBuffer[Double]()
    for(i <- 1 to 3){
      initialize(isTuningParameters = true)
      initializeNeighboursRdd(numberOfNeighboursToFind_, similarityMeasure)
      val usersProducts = testRatingsRddOption_.get.map { case Rating(user, product, rate) =>
        (user, product)
      }
      val predictionsRdd = predict(usersProducts).persist()
      arrayBufferRmse += getRmseWithPredictionsRdd(testRatingsRddOption_.get, predictionsRdd)
      arrayBufferMae += getMaeWithPredictionsRdd(testRatingsRddOption_.get, predictionsRdd)
      predictionsRdd.unpersist()
      neighboursRddOption_.get.unpersist()
      trainRatingsRddOption_.get.unpersist()
      validationRatingsRddOption_.get.unpersist()
    }
    println("Average eval metric RMSE: "+arrayBufferRmse.sum/arrayBufferRmse.size)
    println("Average eval metric MAE: "+arrayBufferMae.sum/arrayBufferMae.size)
  }

  override def predict(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
/*    initializeNeighboursRdd(numberOfNeighboursToFind_, pearsonCorrelation)*/
    predictWithoutNormalization(userProducts)
  }

  private def tuneNumberOfNeighboursWithGridSearch(evaluateMetric: RDD[Rating] => Double): Unit = {
    initializeMeansAndDeviationsRdd()
    if (trainRatingsRddOption_.isDefined && validationRatingsRddOption_.isDefined) {
      var currentBestValidationRmse = evaluateMetric(validationRatingsRddOption_.get)

      for (numberOfNeighbours <- numbersOfNeighboursList) {
        val validationMetricValue = evaluateMetric(validationRatingsRddOption_.get)
        println("Metric (validation) = " + validationMetricValue + " for the model trained with " +
          "number of neighbours = " + numberOfNeighbours + ".")
        if (validationMetricValue < currentBestValidationRmse) {
          currentBestValidationRmse = validationMetricValue
          numberOfNeighboursToFind_ = numberOfNeighbours
        }
      }
      println("The best model was trained with number = " + numberOfNeighboursToFind_
        + ", and its metric on the test set is " + evaluateMetric(testRatingsRddOption_.get) + ".")
    }
    else {
      throw new IllegalStateException()
    }
  }

  def initializeNeighboursRdd(numberOfNeighbours: Int,
                              similarityMeasure: (Array[Double], Array[Double]) => Option[Double]) = {
    val userFeaturesRdd = getUserFeaturesRdd

    val joinedRdd = userFeaturesRdd.cartesian(userFeaturesRdd)
      .filter(tuple => tuple._1._1 < tuple._2._1)
      .map { case ((user1, features1), (user2, features2)) =>
        ((user1, user2), similarityMeasure(features1, features2))
      }.flatMapValues(x => x)
      .map(x => Array((x._1._1, (x._1._2, x._2)), (x._1._2, (x._1._1, x._2))))
      .flatMap(x => x)
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .map { resLine =>
        val arrayOfNeighbours = resLine._2.toArray.sortBy(_._2).reverse.take(numberOfNeighbours)
        (resLine._1, arrayOfNeighbours)
      }
    neighboursRddOption_ = Some(joinedRdd.persist())
  }

  def initializeMeansAndDeviationsRdd(): Unit = {
    meansAndDeviationsRddOption_ = Some(getDeviationAndAveragesRdd(trainRatingsRddOption_.get).persist())
  }

  def initializeMeansRdd(): Unit = {
    meansRddOption_ = Some(getAveragesRdd(trainRatingsRddOption_.get).persist())
  }



  private def predictWithoutNormalization(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    val neighbourBookUserRdd = userProducts
      .join(neighboursRddOption_.get)
      .map { case (user, (book, neighbours)) =>
        ((user, book), neighbours)
      }
      .flatMapValues(x => x)
      .map { case ((user, book), (neighbour, similarity)) =>
        ((neighbour, book), (user, similarity))
      }
    val mapped = trainRatingsRddOption_.get
      .map {
        case Rating(user, book, rate) => ((user, book), rate)
      }
    val resRdd = neighbourBookUserRdd.join(mapped)
      .map { case ((neighbour, book), ((user, similarity), rate)) =>
        ((user, book), (similarity, rate))
      }
      .aggregateByKey(mutable.ArrayBuffer.empty[(Double, Double)])(addToBuffer, mergePartitionBuffers)
      .map { case ((user, book), listOfRates) => ((user, book),
        calculatePredictedRating(listOfRates))
      }
      .flatMapValues(x => x)
      .map { case ((user, book), rating) => new Rating(user, book, rating) }
    resRdd.take(5).foreach(x => println("Final prediction:" + x))
    neighboursRddOption_.get.unpersist()
    resRdd
  }

  private def predictWithZScore(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    val neighbourBookUserRdd = getAveragesAndDeviationsForUsers(userProducts,
      meansAndDeviationsRddOption_.get)
      .join(neighboursRddOption_.get)
      .map { case (user, ((book, average, deviation), neighbours)) =>
        ((user, book, average, deviation), neighbours)
      }
      .flatMapValues(x => x)
      .map { case ((user, book, average, deviation), (neighbour, similarity)) =>
        ((neighbour, book), (user, average, deviation, similarity))
      }
    val mapped = zScoreNormalization(trainRatingsRddOption_.get, meansAndDeviationsRddOption_.get)
      .map {
        case Rating(user, book, rate) => ((user, book), rate)
      }
    /*    mapped.take(5).foreach(x => println("Normalized Rating:" + x))*/
    val resRdd = neighbourBookUserRdd.join(mapped)
      .map { case ((neighbour, book), ((user, average, deviation, similarity), rate)) =>
        ((user, book), List((average, deviation, similarity, rate)))
      }
      .reduceByKey((leftList, rightList) => leftList.++(rightList))
      .map { case ((user, book), listOfRates) => ((user, book),
        calculatePredictedZScoreNormalizedRating(listOfRates))
      }
      .flatMapValues(x => x)
      .map { case ((user, book), rating) => new Rating(user, book, rating) }
    /*    resRdd.take(5).foreach(x => println("Final prediction:" + x))*/
    resRdd.take(100).foreach(x => println("Final prediction:" + x))
    println("Number of predictions: " + resRdd.count())
    neighboursRddOption_.get.unpersist()
    resRdd
  }


  def predictWithMeanCentering(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    val neighbourBookUserRdd = getAveragesForUsers(userProducts, meansRddOption_.get)
      .join(neighboursRddOption_.get)
      .map { case (user, ((book, average), neighbours)) => ((user, book, average), neighbours) }
      .flatMapValues(x => x)
      .map { case ((user, book, average), (neighbour, similarity)) =>
        ((neighbour, book), (user, average, similarity))
      }

    val mapped = meanCenteringNormalization(trainRatingsRddOption_.get, meansRddOption_.get).map {
      case Rating(user, book, rate) => ((user, book), rate)
    }
    val resRdd = neighbourBookUserRdd.join(mapped)
      .map { case ((neighbour, book), ((user, average, similarity), rate)) =>
        ((user, book), (average, similarity, rate))
      }
      .aggregateByKey(mutable.ArrayBuffer.empty[(Double, Double, Double)])(addToBuffer, mergePartitionBuffers)
      .map { case ((user, book), bufferOfRates) =>
        ((user, book), calculatePredictedNormalizedRating(bufferOfRates))
      }
      .flatMapValues(x => x)
      .map { case ((user, book), rating) => new Rating(user, book, rating) }
    resRdd.take(5).foreach(x => println("Final prediction:" + x))
    println("Number of predictions: " + resRdd.count())
    neighboursRddOption_.get.unpersist()
    resRdd
  }


  def calculatePredictedNormalizedRating(bufferOfSimilaritiesAndRates:
                                         mutable.ArrayBuffer[(Double, Double, Double)])
  : Option[Double] = {
    if (bufferOfSimilaritiesAndRates.size > 1) {
      val averageForUser = bufferOfSimilaritiesAndRates.head._1
      val sumOfSimilarities = bufferOfSimilaritiesAndRates.map(_._2).sum
      val normalizedSimilarities = bufferOfSimilaritiesAndRates.map {
        case (average, similarity, rate) => (similarity / sumOfSimilarities, rate)
      }
      Some(averageForUser + (for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum)
    }
    else {
      None
    }
  }

  def calculatePredictedZScoreNormalizedRating(listOfSimilaritiesAndRates: List[(Double, Double, Double, Double)])
  : Option[Double] = {
    if (listOfSimilaritiesAndRates.size > 1) {
      val sumOfSimilarities = listOfSimilaritiesAndRates.map(_._3).sum
      val normalizedSimilarities = listOfSimilaritiesAndRates.map {
        case (average, standardDeviation, similarity, rate) => (similarity / sumOfSimilarities, rate)
      }
      val sum = (for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum

      val sumOfNormalizedSimilarities = normalizedSimilarities.map(_._1).sum

      val prediction = listOfSimilaritiesAndRates.head._1 + (listOfSimilaritiesAndRates.head._2 * sum)/sumOfNormalizedSimilarities
      if (prediction.isNaN) {
        None
      }
      else {
        Some(prediction)
      }
    }
    else {
      None
    }
  }

  def calculatePredictedRating(listOfSimilaritiesAndRates: mutable.ArrayBuffer[(Double, Double)])
  : Option[Double] = {
    if (listOfSimilaritiesAndRates.size > 1) {
      val sumOfSimilarities = listOfSimilaritiesAndRates.map(_._1).sum
      val normalizedSimilarities = listOfSimilaritiesAndRates.map {
        case (similarity, rate) => (similarity / sumOfSimilarities, rate)
      }
      Some((for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum)
    }
    else {
      None
    }
  }

  def meanCenteringNormalization(inputRatingsRdd: RDD[Rating],
                                 meansRdd: RDD[(Int, Double)]): RDD[Rating] = {
    def normalizedRdd = inputRatingsRdd.map {
      case Rating(userId, bookId, rate) => (userId, (bookId, rate))
    }
      .join(meansRdd)
      .map {
        case (userId, ((bookId, rate), average)) =>
          (userId, (bookId, rate - average))
      }
    transformToRatingRdd(normalizedRdd)
  }

  def zScoreNormalization(inputRatingsRdd: RDD[Rating],
                          meansAndDeviationsRdd: RDD[(Int, (Double, Double))]): RDD[Rating] = {
    transformToUserKeyRdd(inputRatingsRdd)
      .join(meansAndDeviationsRdd)
      .filter {
        case (userId, ((bookId, rate), (average, deviation))) => deviation != 0.0
      }
      .map {
        case (userId, ((bookId, rate), (average, deviation))) =>
          Rating(userId, bookId, (rate - average) / deviation)
      }
  }

  def getAveragesAndDeviationsForUsers(userIdsRdd: RDD[(Int, Int)],
                                       meansAndDeviationsRdd: RDD[(Int, (Double, Double))]):
  RDD[(Int, (Int, Double, Double))] = {
    userIdsRdd
      .join(meansAndDeviationsRdd)
      .map { case (userId, (bookId, (average, deviation)))
      => (userId, (bookId, average, deviation))
      }
  }

  def getAveragesForUsers(userIdsRdd: RDD[(Int, Int)],
                          meansRdd: RDD[(Int, Double)]):
  RDD[(Int, (Int, Double))] = {
    userIdsRdd
      .join(meansRdd)
      .map { case (userId, (bookId, average))
      => (userId, (bookId, average))
      }
  }

  def getDeviationAndAveragesRdd(inputRdd: RDD[Rating]): RDD[(Int, (Double, Double))] = {
    val bookIdAverageAndDeviationRdd = transformToUserKeyRdd(inputRdd)
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .map {
        case (userId, setOfRates) =>
          val listOfRates = setOfRates.toList.map(_._2)
          val average = listOfRates.sum / listOfRates.size
          (userId, (average, getDeviation(listOfRates, average)))
      }
    bookIdAverageAndDeviationRdd
  }

  def getAveragesRdd(inputRdd: RDD[Rating]): RDD[(Int, Double)] = {
    val bookIdAverageRdd = transformToUserKeyRdd(inputRdd)
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .map {
        case (userId, setOfRates) =>
          val listOfRates = setOfRates.toList.map(_._2)
          val average = listOfRates.sum / listOfRates.size
          (userId, average)
      }
    bookIdAverageRdd
  }

  def getDeviation(rates: List[Double], mean: Double): Double = {
    val deviation = Math.sqrt((for {rate <- rates} yield Math.pow(rate - mean, 2)).sum
      / rates.size)
    deviation
  }

  def getDeviation(rates: List[Double]): Double = {
    val mean = rates.sum / rates.size
    val deviation = Math.sqrt((for {rate <- rates} yield Math.pow(rate - mean, 2)).sum
      / rates.length)
    deviation
  }

  def cosineSimilarity(x: Array[Double], y: Array[Double]): Option[Double] = {
    require(x.length == y.length)
    val res = dotProduct(x, y) / (magnitude(x) * magnitude(y))
    if (res.isNaN) None
    else Some(res)
  }

  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for ((a, b) <- x zip y) yield a * b) sum
  }

  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map (i => i * i) sum)
  }

  private def pearsonCorrelation(x: Array[Double], y: Array[Double]):
  Option[Double] = {
    calculatePearsonCorrelation(x.size, dotProduct(x, y), x.sum, y.sum,
      x.map { case rate: Double => rate * rate }.sum, y.map { case rate: Double => rate * rate }.sum)
  }

  private def calculatePearsonCorrelation(groupSize: Int, dotProduct: Double, rating1Sum: Double, rating2Sum: Double,
                                          rating1NormSq: Double, rating2NormSq: Double): Option[Double] = {
    val numerator = groupSize * dotProduct - rating1Sum * rating2Sum
    val denominator = Math.sqrt(groupSize * rating1NormSq - rating1Sum * rating1Sum) *
      Math.sqrt(groupSize * rating2NormSq - rating2Sum * rating2Sum)
    val res = numerator / denominator
    if (res.isNaN) None
    else Some(res)
  }
}
