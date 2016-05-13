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

  val numbersOfNeighboursList = List(25, 30, 35, 40, 45)

  private var numberOfNeighboursToFind_ = 50

  protected var meansAndDeviationsRddOption_ : Option[RDD[(Int, (Double, Double))]] = None
  private var neighboursRddOption_ : Option[RDD[(Int, Array[(Int, Double)])]] = None


  override def test(): Unit = {
    initialize(isTuningParameters = true, getMaeForRdd)
    /*    println("Train RMSE = " + getRmseForRdd(trainRatingsRddOption_.get))*/
    tuneNumberOfNeighboursWithGridSearch(getMaeForRdd)
    println("Test RMSE = " + getMaeForRdd(testRatingsRddOption_.get))
    neighboursRddOption_.get.unpersist()
    trainRatingsRddOption_.get.unpersist()
    validationRatingsRddOption_.get.unpersist()
    meansAndDeviationsRddOption_.get.unpersist()
  }

  private def tuneNumberOfNeighboursWithGridSearch(evaluateMetric: RDD[Rating] => Double): Unit = {
    initializeNeighboursRdd(numberOfNeighboursToFind_, cosineSimilarity)
    initializeMeansAndDeviationsRdd()
    if (trainRatingsRddOption_.isDefined && neighboursRddOption_.isDefined &&
      validationRatingsRddOption_.isDefined) {

      var currentBestValidationRmse = evaluateMetric(validationRatingsRddOption_.get)

      for (numberOfNeighbours <- numbersOfNeighboursList) {
        initializeNeighboursRdd(numberOfNeighbours, cosineSimilarity)
        val validationMetric = evaluateMetric(validationRatingsRddOption_.get)
        println("Metric (validation) = " + validationMetric + " for the model trained with " +
          "number of neighbours = " + numberOfNeighbours + ".")
        if (validationMetric < currentBestValidationRmse) {
          currentBestValidationRmse = validationMetric
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

  override def tuneHyperParametersWithRandomSearch(evaluateMetric: RDD[Rating] => Double): Unit = {
    initializeMeansAndDeviationsRdd()
    super.tuneHyperParametersWithRandomSearch(evaluateMetric)
  }

  override def tuneHyperParametersWithGridSearch(evaluateMetric: RDD[Rating] => Double): Unit = {
    initializeMeansAndDeviationsRdd()
    super.tuneHyperParametersWithGridSearch(evaluateMetric)
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

  override def predict(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    predictWithZScore(userProducts)
  }

  private def predictWithZScore(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    initializeNeighboursRdd(numberOfNeighboursToFind_, cosineSimilarity)

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
    resRdd
  }

  private def predictWithoutNormalization(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
     initializeNeighboursRdd(numberOfNeighboursToFind_, cosineSimilarity)

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
    /*    mapped.take(5).foreach(x => println("Normalized Rating:" + x))*/
    val resRdd = neighbourBookUserRdd.join(mapped)
      .map { case ((neighbour, book), ((user, similarity), rate)) =>
        ((user, book), List((similarity, rate)))
      }
      .reduceByKey((leftList, rightList) => leftList.++(rightList))
      .map { case ((user, book), listOfRates) => ((user, book),
        calculatePredictedRating(listOfRates))
      }
      .flatMapValues(x => x)
      .map { case ((user, book), rating) => new Rating(user, book, rating) }
    /*    resRdd.take(5).foreach(x => println("Final prediction:" + x))*/
    resRdd
  }

  /*  def predict1(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
      val inputRdd = getUnitedInputRdd()

      val neighbourBookUserRdd = getAveragesForUsers(userProducts, inputRdd)
        .join(neighboursRdd.get)
        .map { case (user, ((book, average), neighbours)) => ((user, book, average), neighbours) }
        .flatMapValues(x => x)
        .map { case ((user, book, average), (neighbour, similarity)) =>
          ((neighbour, book), (user, average, similarity))
        }

      val mapped = meanCenteringNormalization(inputRdd).map {
        case Rating(user, book, rate) => ((user, book), rate)
      }
      val resRdd = neighbourBookUserRdd.join(mapped)
        .map { case ((neighbour, book), ((user, average, similarity), rate)) =>
          ((user, book), Set((average, similarity, rate)))
        }
        .reduceByKey((leftSet, rightSet) => leftSet.++(rightSet))
        .map { case ((user, book), setOfRates) => ((user, book), calculatePredictedNormalizedRating(setOfRates)) }
        .flatMapValues(x => x)
        .map { case ((user, book), rating) => new Rating(user, book, rating) }
      resRdd
    }*/


  /*  def calculatePredictedNormalizedRating(setOfSimilaritiesAndRates: Set[(Double, Double, Double)])
    : Option[Double] = {
      if (setOfSimilaritiesAndRates.size > 1) {
        val averageForUser = setOfSimilaritiesAndRates.head._1
        val sumOfSimilarities = setOfSimilaritiesAndRates.map(_._2).sum
        val normalizedSimilarities = setOfSimilaritiesAndRates.map {
          case (average, similarity, rate) => (similarity / sumOfSimilarities, rate)
        }
        Some(averageForUser + (for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum)
      }
      else {
        None
      }
    }*/

  def calculatePredictedZScoreNormalizedRating(listOfSimilaritiesAndRates: List[(Double, Double, Double, Double)])
  : Option[Double] = {
    if (listOfSimilaritiesAndRates.size > 1) {
      val sumOfSimilarities = listOfSimilaritiesAndRates.map(_._3).sum
      val normalizedSimilarities = listOfSimilaritiesAndRates.map {
        case (average, standardDeviation, similarity, rate) => (similarity / sumOfSimilarities, rate)
      }
      val sum = (for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum
      val prediction = listOfSimilaritiesAndRates.head._1 + listOfSimilaritiesAndRates.head._2 * sum
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

    def calculatePredictedRating(listOfSimilaritiesAndRates: List[(Double, Double)])
    : Option[Double] = {
      if (listOfSimilaritiesAndRates.size > 1) {
        val sumOfSimilarities = listOfSimilaritiesAndRates.map(_._2).sum
        val normalizedSimilarities = listOfSimilaritiesAndRates.map {
          case (similarity, rate) => (similarity / sumOfSimilarities, rate)
        }
        Some((for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum)
      }
      else {
        None
      }
    }

  /*  def meanCenteringNormalization(inputRatingsRdd: RDD[Rating]): RDD[Rating] = {
      def normalizedRdd = inputRatingsRdd.map {
        case Rating(userId, bookId, rate) => (userId, Set((bookId, rate)))
      }
        .reduceByKey((leftSet, rightSet) => leftSet.++(rightSet))
        .map {
          case (userId, setOfPairs) =>
            val average = setOfPairs.map(_._2).sum / setOfPairs.size
            (userId, setOfPairs.map {
              case (bookId, rate) => (bookId, rate - average)
            })
        }
        .flatMapValues(x => x)
      transformToRatingRdd(normalizedRdd)
    }*/

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

  /*  def getAveragesForUsers(userIdsRdd : RDD[(Int,Int)], inputRdd: RDD[Rating]): RDD[(Int, (Int, Double))] ={
      val averagesRdd = transformToUserKeyRdd(inputRdd).join(userIdsRdd).map{
        case (userId, ((bookId, rate), bookId2)) => (userId, Set((bookId, rate)))
      }
        .reduceByKey((leftSet, rightSet) => leftSet.++(rightSet))
        .map {
          case (userId, setOfPairs) =>
            val average = setOfPairs.map(_._2).sum / setOfPairs.size
            (userId, setOfPairs.map {
              case (bookId, rate) => (bookId, average)
            })
        }.flatMapValues(x => x)
      averagesRdd
    }*/

  def getAveragesAndDeviationsForUsers(userIdsRdd: RDD[(Int, Int)],
                                       meansAndDeviationsRdd: RDD[(Int, (Double, Double))]):
  RDD[(Int, (Int, Double, Double))] = {
    userIdsRdd
      .join(meansAndDeviationsRdd)
      .map { case (userId, (bookId, (average, deviation)))
      => (userId, (bookId, average, deviation))
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

  def getDeviation(rates: List[Double], mean: Double): Double = {
    val deviation = Math.sqrt((for {rate <- rates} yield Math.pow(rate - mean, 2)).sum
      / rates.size - 1)
    deviation
  }

  def getDeviation(rates: List[Double]): Double = {
    val mean = rates.sum / rates.size
    val deviation = Math.sqrt((for {rate <- rates} yield Math.pow(rate - mean, 2)).sum
      / rates.length - 1)
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
}
