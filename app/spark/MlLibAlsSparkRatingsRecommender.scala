package spark

import javax.inject.{Inject, Singleton}

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import play.api.Configuration

import scala.collection.mutable
import scala.util.Random

/**
  * Created by P. Akhmedzianov on 03.03.2016.
  */
@Singleton
class MlLibAlsSparkRatingsRecommender @Inject()(val configuration: Configuration)
  extends SparkMongoHandler(configuration) with java.io.Serializable with EvaluationMetrics {

  val configurationPath_ = configuration.getString(
    "mlLibAlsSparkRatingsRecommender.configurationPath").getOrElse(
    SparkAlsPropertiesLoader.defaultPath)

  val numberOfRecommendedBooks_ = configuration.getInt(
    "mlLibAlsSparkRatingsRecommender.numberOfRecommendedBooks").getOrElse(5)

  val minNumberOfRatesToGetRecommendations_ = configuration.getInt(
    "mlLibAlsSparkRatingsRecommender.minNumberOfRatesToGetRecommendations").getOrElse(10)

  private var alsConfigurationOption_ : Option[AlsConfiguration] = None

  private var matrixFactorizationModelOption_ : Option[MatrixFactorizationModel] = None

  protected val trainRatingsRddPath_ = "G:\\input\\trainRatingsRdd"
  protected val validationRatingsRddPath_ = "G:\\input\\validationRatingsRdd"
  protected val testRatingsRddPath_ = "G:\\input\\testRatingsRdd"

  protected var trainRatingsRddOption_ : Option[RDD[Rating]] = None
  protected var validationRatingsRddOption_ : Option[RDD[Rating]] = None
  protected var testRatingsRddOption_ : Option[RDD[Rating]] = None

  def updateRecommendationsInMongo(isTuning: Boolean): Unit = {
    initialize(isTuning, getRmseForRdd)
    exportAllPredictionsToMongo(numberOfRecommendedBooks_)
        trainRatingsRddOption_.get.unpersist()
    if (isTuning) {
            validationRatingsRddOption_.get.unpersist()
            testRatingsRddOption_.get.unpersist()
    }
  }

  def test(): Unit = {
    initialize(isTuningParameters = true)
    matrixFactorizationModelOption_ match {
      case Some(matrixModel) =>
        println("Train RMSE = " + getRmseForRdd(trainRatingsRddOption_.get))
        println("Validation RMSE = " + getRmseForRdd(validationRatingsRddOption_.get))
        println("Test MAP@K = " + getMeanAveragePrecisionForRdd(testRatingsRddOption_.get))
      case None => throw new IllegalStateException()
    }
  }

  protected def initialize(isTuningParameters: Boolean,
                           evaluateMetric: RDD[Rating] => Double = getMaeForRdd): Unit = {
    alsConfigurationOption_ = Some(SparkAlsPropertiesLoader.loadFromDisk(configurationPath_))
    initializeRdds(filterInputByNumberOfKeyEntriesRdd(getKeyValueRatings(
      getCollectionFromMongoRdd(ratingsCollectionName_)), minNumberOfRatesToGetRecommendations_),
      isTuningParameters)
/*    if (isTuningParameters) {
      tuneHyperParametersWithGridSearch(evaluateMetric)
      tuneHyperParametersWithRandomSearch(evaluateMetric)
    }*/
    initializeModelWithRdd(trainRatingsRddOption_.get)
  }


  private def initializeModelWithRdd(ratingsRdd: RDD[Rating]) = {
    if (alsConfigurationOption_.isDefined) {
      matrixFactorizationModelOption_ = Some(ALS.train(ratingsRdd, alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.numIterations_, alsConfigurationOption_.get.lambda_))
    } else {
      throw new IllegalStateException()
    }
  }

  private def initializeRdds(filteredInputRdd: RDD[(Int, (Int, Double))],
                             isTuning: Boolean): Unit = {
    if (isTuning && alsConfigurationOption_.isDefined) {
      val trainRdd = filteredInputRdd
        .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
        .map { groupedLine =>
          val movieRatePairsArray = groupedLine._2.toArray
          (groupedLine._1, movieRatePairsArray.splitAt(Math.ceil(
            alsConfigurationOption_.get.trainingShareInArraysOfRates_ * movieRatePairsArray.length).toInt)._1)
        }.flatMapValues(x => x)

      trainRatingsRddOption_ = Some(transformToRatingRdd(trainRdd).persist())
      println("Train size: "+trainRatingsRddOption_.get.count())
      val afterSubtractionRdd = filteredInputRdd.subtract(trainRdd)

      val validateAndTestRdds = afterSubtractionRdd.randomSplit(
        Array(alsConfigurationOption_.get.validationShareAfterSubtractionTraining_,
          1 - alsConfigurationOption_.get.validationShareAfterSubtractionTraining_))
      validationRatingsRddOption_ = Some(transformToRatingRdd(validateAndTestRdds(0)).persist())
      println("Validation size: "+validationRatingsRddOption_.get.count())
      testRatingsRddOption_ = Some(transformToRatingRdd(validateAndTestRdds(1)).persist())
      println("Test size: "+testRatingsRddOption_.get.count())
    }
    else {
      trainRatingsRddOption_ = Some(transformToRatingRdd(filteredInputRdd).persist())
    }
  }

  override def predict(userProducts: RDD[(Int, Int)]): RDD[Rating] = {
    if (matrixFactorizationModelOption_.isDefined) {
      matrixFactorizationModelOption_.get.predict(userProducts)
    }
    else {
      throw new IllegalStateException("Matrix factorization model is not defined!")
    }
  }

  private def getMeanAveragePrecisionForRdd(ratings: RDD[Rating]): Double = {
    if (matrixFactorizationModelOption_.isDefined) {
      // Evaluate the model on rating data
      val K = 10
      val usersWithPositiveRatings = ratings.filter { case Rating(user, product, rate) => rate > 5 }.map {
        case Rating(user, product, rate) =>
          (user, product)
      }.groupByKey()
      val predictions = matrixFactorizationModelOption_.get.recommendProductsForUsers(K).map {
        case (userId, ratingPredictions) => (userId, ratingPredictions.map {
          case Rating(user, product, rate) => product
        })
      }
      val trueBooksWithPositiveRateAndPredictedBooks = usersWithPositiveRatings.join(predictions)
      val meanAveragePrecision = trueBooksWithPositiveRateAndPredictedBooks
        .map { case (user, (trueBooks, predictedBooks)) =>
          val trueBooksArray = trueBooks.toArray
          val minNumberOfTrueRatesAndK = Math.min(trueBooksArray.length, K)
          var precision = 0
          for (k <- predictedBooks.indices) {
            val addition = if (trueBooksArray.contains(predictedBooks(k))) k else 0
            precision += addition
          }
          precision.toDouble / minNumberOfTrueRatesAndK
        }.mean()
      meanAveragePrecision
    }
    else {
      throw new IllegalStateException()
    }
  }

  private def exportAllPredictionsToMongo(numberOfRecommendations: Int): Unit = {
    if (matrixFactorizationModelOption_.isDefined) {
      val predictionsRdd = matrixFactorizationModelOption_.get.recommendProductsForUsers(
        numberOfRecommendations).mapValues { arrayRatings => arrayRatings.map {
        case Rating(userId, bookId, rate) => bookId
      }
      }
      updateMongoCollectionWithRdd(usersCollectionName_,
        predictionsRdd.map { resTuple =>
          (new Object, getMongoUpdateWritableFromIdValueTuple[Int, Array[Int]](resTuple, "_id",
            "personalRecommendations"))
        })
    }
  }

  // also filtering implicit ratings in dataset (removing zeros)
  def filterInputByNumberOfKeyEntriesRdd(inputRdd: RDD[(Int, (Int, Double))],
                                         threshold: Int): RDD[(Int, (Int, Double))] = {
    val filteredRatingsRdd = inputRdd
      .filter { case (userId, (bookId, rate)) => rate != 0.0 }
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .filter { groupedLine =>
        groupedLine._2.size >= threshold
      }
      .flatMapValues(x => x)
    println("Count after filtering:" + filteredRatingsRdd.count())
    filteredRatingsRdd
  }

  protected def getUserFeaturesRdd: RDD[(Int, Array[Double])] = {
    if (matrixFactorizationModelOption_.isDefined) {
      matrixFactorizationModelOption_.get.userFeatures
    }
    else {
      throw new IllegalStateException()
    }
  }

  protected def tuneHyperParametersWithGridSearch(evaluateMetric: RDD[Rating] => Double): Unit = {
    tuneHyperParameters(evaluateMetric, gridSearch)
  }

  protected def tuneHyperParametersWithRandomSearch(evaluateMetric: RDD[Rating] => Double): Unit = {
    tuneHyperParameters(evaluateMetric, randomSearch)
  }

  // tuning only number of factors and lambda
  protected def tuneHyperParameters(evaluateMetric: RDD[Rating] => Double,
                                    searchFunction: (Double, RDD[Rating] => Double) => Unit): Unit = {
    if (alsConfigurationOption_.isDefined && trainRatingsRddOption_.isDefined &&
      validationRatingsRddOption_.isDefined) {
      matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.numIterations_, alsConfigurationOption_.get.lambda_))
      val currentBestValidationMetricValue = evaluateMetric(validationRatingsRddOption_.get)

      searchFunction(currentBestValidationMetricValue, evaluateMetric)

      println("The best model was trained with rank = " + alsConfigurationOption_.get.rank_ +
        " and lambda = " + alsConfigurationOption_.get.lambda_ + ", and numIter = " +
        alsConfigurationOption_.get.numIterations_ + ", and its Eval metric on the test set is "
        + evaluateMetric(testRatingsRddOption_.get) + ".")
      SparkAlsPropertiesLoader.saveToDisk(configurationPath_, alsConfigurationOption_.get)
    }
    else {
      throw new IllegalStateException()
    }
  }

  private def randomSearch(currentBestMetricValue: Double, evaluateMetric: RDD[Rating] => Double): Unit = {
    var currentBestValidationMetricValue = currentBestMetricValue
    for (i <- 1 to alsConfigurationOption_.get.numberOfSearchSteps_) {
      val nextPosition = chooseNextWithRandomAngle((alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.lambda_), alsConfigurationOption_.get.stepRadius_)
      matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, nextPosition._1,
        alsConfigurationOption_.get.numIterations_, nextPosition._2))
      val validationEvaluateMetric = evaluateMetric(validationRatingsRddOption_.get)
      println("#" + i + " Eval metric (validation) = " + validationEvaluateMetric + " for the model trained with rank = "
        + nextPosition._1 + ", lambda = " + nextPosition._2 + ", and numIter = " +
        alsConfigurationOption_.get.numIterations_ + ".")

      if (validationEvaluateMetric < currentBestValidationMetricValue) {
        currentBestValidationMetricValue = validationEvaluateMetric
        alsConfigurationOption_ = Some(alsConfigurationOption_.get.copy(rank_ = nextPosition._1,
          lambda_ = nextPosition._2))
      }
    }
  }

  private def gridSearch(currentBestMetricValue: Double, evaluateMetric: RDD[Rating] => Double): Unit = {
    var currentBestValidationMetricValue = currentBestMetricValue
    for (rank <- alsConfigurationOption_.get.ranksList_;
         lambda <- alsConfigurationOption_.get.lambdasList_;
         numberOfIterations <- alsConfigurationOption_.get.numbersOfIterationsList_) {
      matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, rank, numberOfIterations, lambda))
      val validationEvaluateMetric = evaluateMetric(validationRatingsRddOption_.get)
      println("Eval metric (validation) = " + validationEvaluateMetric + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numberOfIterations + ".")
      if (validationEvaluateMetric < currentBestValidationMetricValue) {
        currentBestValidationMetricValue = validationEvaluateMetric
        alsConfigurationOption_ = Some(alsConfigurationOption_.get.copy(rank_ = rank,
          numIterations_ = numberOfIterations, lambda_ = lambda))
      }
    }
  }

  private def chooseNextWithRandomAngle(currentPosition: (Int, Double),
                                        radius: Double): (Int, Double) = {
    var newPositionX: Double = -1.0
    var newPositionY: Double = -1.0
    while (newPositionX < 1.0 || newPositionY < 0.0) {
      val angle = 2 * Math.PI * Random.nextDouble()
      newPositionX = currentPosition._1 + Math.cos(angle) * radius
      newPositionY = currentPosition._2 + Math.sin(angle) * radius
    }
    (Math.round(newPositionX).toInt, newPositionY)
  }
}
