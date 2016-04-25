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

  val numbersOfNeighboursList = List(50, 100, 150)

  private var numberOfNeighboursToFind_ = 50

  private def tuneNumberOfNeighboursWithGridSearch(): Unit = {
    initializeNeighboursRdd(numberOfNeighboursToFind_)
    if (trainRatingsRddOption_.isDefined && neighboursRdd.isDefined &&
      validationRatingsRddOption_.isDefined) {

      var currentBestValidationRmse = getRmseForRdd(validationRatingsRddOption_.get)

      for (numberOfNeighbours <- numbersOfNeighboursList) {
        initializeNeighboursRdd(numberOfNeighbours)
        val validationRmse = getRmseForRdd(validationRatingsRddOption_.get)
        println("RMSE (validation) = " + validationRmse + " for the model trained with " +
          "number of neighbours = " + numberOfNeighbours + ".")
        if (validationRmse < currentBestValidationRmse) {
          currentBestValidationRmse = validationRmse
          numberOfNeighboursToFind_ = numberOfNeighbours
        }
      }
      println("The best model was trained with number = " + numberOfNeighboursToFind_
        + ", and its RMSE on the test set is " + currentBestValidationRmse + ".")
    }
    else {
      throw new IllegalStateException()
    }
  }

  private var neighboursRdd: Option[RDD[(Int, Array[(Int, Double)])]] = None

  override def test(): Unit = {
    initialize(isTuningParameters = false, isInitializingValidationRdd = true,
      isInitializingTestRdd = true)
    initializeNeighboursRdd(numberOfNeighboursToFind_)
    /*    println("Train RMSE = " + getRmseForRdd(trainRatingsRddOption_.get))*/
    println("Validation RMSE = " + getRmseForRdd(validationRatingsRddOption_.get))
    neighboursRdd.get.unpersist()
    trainRatingsRddOption_.get.unpersist()
    validationRatingsRddOption_.get.unpersist()
  }

  def initializeNeighboursRdd(numberOfNeighbours: Int) = {
    val userFeaturesRdd = getUserFeaturesRdd()

    val joinedRdd = userFeaturesRdd.cartesian(userFeaturesRdd)
      .filter(tuple => tuple._1._1 < tuple._2._1)
      .map { case ((user1, features1), (user2, features2)) =>
        ((user1, user2), cosineSimilarity(features1, features2))
      }.flatMapValues(x => x)
      .map(x => Array((x._1._1, (x._1._2, x._2)), (x._1._2, (x._1._1, x._2))))
      .flatMap(x => x)
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .map { resLine =>
        val arrayOfNeighbours = resLine._2.toArray.sortBy(_._2).reverse.take(numberOfNeighbours)
        (resLine._1, arrayOfNeighbours)
      }
    neighboursRdd = Some(joinedRdd.persist())
  }

  def getNeighbours(userId: Int): Array[Int] = {
    val sequenceUserNeighbours = neighboursRdd.get.lookup(userId)
    if (sequenceUserNeighbours.size == 1) sequenceUserNeighbours.head.map(_._1)
    else throw new Exception("Wrong number")
  }

  override def predict(userId: Int, bookId: Int): Double = {
    val broadcastNeighboursVar = SparkCommons.sc.broadcast(getNeighbours(userId))

    val filteredRatings = trainRatingsRddOption_.get.filter {
      case Rating(userId, book, rate) => broadcastNeighboursVar.value.contains(userId) && bookId == book
    }
      .map(_.rating)
    filteredRatings.mean()
  }

  override def predict(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    val inputRdd = getUnitedInputRdd().cache()
    val meansAndDeviationsRdd = getDeviationAndAveragesRdd(inputRdd).cache()

    val neighbourBookUserRdd = getAveragesAndDeviationsForUsers(userProducts, meansAndDeviationsRdd)
      .join(neighboursRdd.get)
      .map { case (user, ((book, average, deviation), neighbours)) => ((user, book, average, deviation), neighbours) }
      .flatMapValues(x => x)
      .map { case ((user, book, average, deviation), (neighbour, similarity)) =>
        ((neighbour, book), (user, average, deviation, similarity))
      }

    val mapped = zScoreNormalization(inputRdd, meansAndDeviationsRdd).map {
      case Rating(user, book, rate) => ((user, book), rate)
    }
    inputRdd.unpersist()
    meansAndDeviationsRdd.unpersist()
    mapped.take(5).foreach(x => println("Normalized Rating:" + x))
    val resRdd = neighbourBookUserRdd.join(mapped)
      .map { case ((neighbour, book), ((user, average, deviation, similarity), rate)) =>
        ((user, book), Set((average, deviation, similarity, rate)))
      }
      .reduceByKey((leftSet, rightSet) => leftSet.++(rightSet))
      .map { case ((user, book), setOfRates) => ((user, book),
        calculatePredictedZScoreNormalizedRating(setOfRates))
      }
      .flatMapValues(x => x)
      .map { case ((user, book), rating) => new Rating(user, book, rating) }
    resRdd.take(5).foreach(x => println("Final prediction:" + x))
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

  def calculatePredictedZScoreNormalizedRating(setOfSimilaritiesAndRates: Set[(Double, Double, Double, Double)])
  : Option[Double] = {
    if (setOfSimilaritiesAndRates.size > 1) {
      val arrayOfTuples = setOfSimilaritiesAndRates.toArray
      val sumOfSimilarities = arrayOfTuples.map(_._3).sum
      val normalizedSimilarities = arrayOfTuples.map {
        case (average, standartDeviation, similarity, rate) => (similarity / sumOfSimilarities, rate)
      }
      val sum = (for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum
      val prediction = arrayOfTuples(0)._1 + arrayOfTuples(0)._2 * sum
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

  /*  def calculatePredictedRating(setOfSimilaritiesAndRates: Set[(Double, Double)])
    : Option[Double] = {
      if (setOfSimilaritiesAndRates.size > 1) {
        val sumOfSimilarities = setOfSimilaritiesAndRates.map(_._2).sum
        val normalizedSimilarities = setOfSimilaritiesAndRates.map {
          case (similarity, rate) => (similarity / sumOfSimilarities, rate)
        }
        Some((for {tuple <- normalizedSimilarities} yield tuple._1 * tuple._2).sum)
      }
      else {
        None
      }
    }*/

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
    userIdsRdd.join(meansAndDeviationsRdd).map {
      case (userId, (bookId, (average, deviation))) => (userId, (bookId, average, deviation))
    }
  }

  def getDeviationAndAveragesRdd(inputRdd: RDD[Rating]): RDD[(Int, (Double, Double))] = {
    val bookIdAverageAndDeviationRdd = transformToUserKeyRdd(inputRdd).map {
      case (userId, (bookId, rate)) => (userId, Set(rate))
    }
      .reduceByKey((leftSet, rightSet) => leftSet.++(rightSet))
      .map {
        case (userId, setOfPairs) =>
          val rates = setOfPairs.toArray
          val average = rates.sum / rates.size
          (userId, rates.map {
            case (rate) => (average, getDeviation(rates, average))
          })
      }.flatMapValues(x => x)
    bookIdAverageAndDeviationRdd
  }

  def getDeviation(rates: Array[Double], mean: Double): Double = {
    val deviation = Math.sqrt((for {rate <- rates} yield Math.pow(rate - mean, 2)).sum
      / rates.length - 1)
    deviation
  }

  def getDeviation(rates: Array[Double]): Double = {
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
