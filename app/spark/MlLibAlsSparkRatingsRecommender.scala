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
  extends SparkMongoHandler(configuration) with java.io.Serializable with EvaluationMetrics{

  val configurationPath_ = configuration.getString(
    "mlLibAlsSparkRatingsRecommender.configurationPath").getOrElse(
    SparkAlsPropertiesLoader.defaultPath)

  val numberOfRecommendedBooks_ = configuration.getInt(
    "mlLibAlsSparkRatingsRecommender.numberOfRecommendedBooks").getOrElse(5)

  val minNumberOfRatesToGetRecommendations_ = configuration.getInt(
    "mlLibAlsSparkRatingsRecommender.minNumberOfRatesToGetRecommendations").getOrElse(10)

  private var alsConfigurationOption_ : Option[AlsConfiguration] = None

  private var matrixFactorizationModelOption_ : Option[MatrixFactorizationModel] = None

  protected var trainRatingsRddOption_ : Option[RDD[Rating]] = None
  protected var validationRatingsRddOption_ : Option[RDD[Rating]] = None
  protected var testRatingsRddOption_ : Option[RDD[Rating]] = None

  def updateRecommendationsInMongo(isTuning : Boolean): Unit ={
    initialize(isTuning, isTuning, isInitializingTestRdd = false)
    exportAllPredictionsToMongo(numberOfRecommendedBooks_)
  }

  def test(): Unit = {
    initialize (isTuningParameters = true, isInitializingValidationRdd = true,
      isInitializingTestRdd = true)
    matrixFactorizationModelOption_ match {
      case Some(matrixModel) =>
        println("Train RMSE = " + getRmseForRdd(trainRatingsRddOption_.get))
        println("Validation RMSE = " + getRmseForRdd(validationRatingsRddOption_.get))
        println("Test MAP@K = " + getMeanAveragePrecisionForRdd(testRatingsRddOption_.get))
      case None => throw new IllegalStateException()
    }
  }

  protected def initialize(isTuningParameters: Boolean, isInitializingValidationRdd: Boolean,
                 isInitializingTestRdd: Boolean): Unit = {
    if (isTuningParameters && !isInitializingValidationRdd){
      throw new IllegalArgumentException
    }
    else {
      alsConfigurationOption_ = Some(SparkAlsPropertiesLoader.loadFromDisk(configurationPath_))
      initializeRdds(filterInputByNumberOfKeyEntriesRdd(getKeyValueRatings(
        getCollectionFromMongoRdd(ratingsCollectionName_)),minNumberOfRatesToGetRecommendations_),
        isInitializingValidationRdd, isInitializingTestRdd)
      if (isTuningParameters) tuneHyperParametersWithRandomSearch(getRmseForRdd,
        alsConfigurationOption_.get.numberOfSearchSteps_, alsConfigurationOption_.get.stepRadius_)
      initializeModelWithRdd(trainRatingsRddOption_.get)
    }
  }


  private def initializeModelWithRdd(ratingsRdd: RDD[Rating]) = {
    if(alsConfigurationOption_.isDefined) {
      matrixFactorizationModelOption_ = Some(ALS.train(ratingsRdd, alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.numIterations_, alsConfigurationOption_.get.lambda_))
    } else{
      throw new IllegalStateException()
    }
  }

  private def initializeModelWithAllData(): Unit ={
    if(trainRatingsRddOption_.isDefined && alsConfigurationOption_.isDefined){
      matrixFactorizationModelOption_ = Some(ALS.train(getUnitedInputRdd(), alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.numIterations_, alsConfigurationOption_.get.lambda_))
    }
    else{
      throw new IllegalStateException()
    }
  }

  protected  def getUnitedInputRdd():RDD[Rating]={
    var unitedRdd = trainRatingsRddOption_.get
    if(validationRatingsRddOption_.isDefined) unitedRdd = unitedRdd.union(validationRatingsRddOption_.get)
    if(testRatingsRddOption_.isDefined) unitedRdd = unitedRdd.union(testRatingsRddOption_.get)
    unitedRdd
  }


  private def initializeRdds(filteredInputRdd: RDD[(Int, (Int, Double))],
                             isInitializingValidationRdd: Boolean,
                             isInitializingTestRdd: Boolean): Unit = {
    if ((isInitializingValidationRdd || isInitializingTestRdd) && alsConfigurationOption_.isDefined) {
      val gropedByKeyRdd = filteredInputRdd.groupByKey()

      val trainRdd = gropedByKeyRdd.map { groupedLine =>
        val movieRatePairsArray = groupedLine._2.toArray
        (groupedLine._1, movieRatePairsArray.splitAt(Math.ceil(
          alsConfigurationOption_.get.trainingShareInArraysOfRates_ * movieRatePairsArray.length).toInt)._1)
      }.flatMapValues(x => x)

      trainRatingsRddOption_ = Some(transformToRatingRdd(trainRdd).cache())

      val afterSubtractionRdd = filteredInputRdd.subtract(trainRdd)
      if (isInitializingValidationRdd && isInitializingTestRdd) {
        val validateAndTestRdds = afterSubtractionRdd.randomSplit(
          Array(alsConfigurationOption_.get.validationShareAfterSubtractionTraining_,
            1 - alsConfigurationOption_.get.validationShareAfterSubtractionTraining_))
        validationRatingsRddOption_ = Some(transformToRatingRdd(validateAndTestRdds(0)).cache())
        testRatingsRddOption_ = Some(transformToRatingRdd(validateAndTestRdds(1)).cache())
      }
      else if (isInitializingValidationRdd) {
        validationRatingsRddOption_ = Some(transformToRatingRdd(afterSubtractionRdd).cache())
      }
      else {
        testRatingsRddOption_ = Some(transformToRatingRdd(afterSubtractionRdd).cache())
      }
    }
    else {
      trainRatingsRddOption_ = Some(transformToRatingRdd(filteredInputRdd).cache())
    }
  }

  override def predict(userProducts:RDD[(Int, Int)]):RDD[Rating]={
    if(matrixFactorizationModelOption_.isDefined) {
      matrixFactorizationModelOption_.get.predict(userProducts)
    }
    else{
      throw  new IllegalStateException("Matrix factorization model is not defined!")
    }
  }

  private def getMeanAveragePrecisionForRdd(ratings: RDD[Rating]): Double = {
    if(matrixFactorizationModelOption_.isDefined) {
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
    else{
      throw new IllegalStateException()
    }
  }

  private def exportAllPredictionsToMongo(numberOfRecommendations:Int): Unit = {
    if(matrixFactorizationModelOption_.isDefined) {
      val predictionsRdd  = matrixFactorizationModelOption_.get.recommendProductsForUsers(
        numberOfRecommendations).mapValues{arrayRatings => arrayRatings.map{
        case Rating(userId, bookId, rate) => bookId
      }}
      updateMongoCollectionWithRdd(usersCollectionName_,
        predictionsRdd.map { resTuple =>
          (new Object, getMongoUpdateWritableFromIdValueTuple[Int, Array[Int]](resTuple, "_id",
            "personalRecommendations"))
        })
    }
  }

  def filterInputByNumberOfKeyEntriesRdd(inputRdd: RDD[(Int, (Int, Double))],
                                         threshold: Int): RDD[(Int, (Int, Double))] = {
    val filteredRatingsRdd = inputRdd
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .filter { groupedLine =>
        groupedLine._2.size >= threshold
      }
      .flatMapValues(x => x)
    filteredRatingsRdd
  }

  protected def getUserFeaturesRdd:RDD[(Int, Array[Double])]={
    if(matrixFactorizationModelOption_.isDefined) {
      matrixFactorizationModelOption_.get.userFeatures
    }
    else{
      throw new IllegalStateException()
    }
  }

  def predict(user: Int, product: Int): Double = {
    matrixFactorizationModelOption_ match {
      case Some(matrixModel) =>
        matrixModel.predict(user, product)
      case None => throw new IllegalStateException()
    }
  }

  def recommendProducts(user: Int, num: Int): Map[Int, Double] = {
    matrixFactorizationModelOption_ match {
      case Some(matrixModel) =>
        def resFromModel = matrixModel.recommendProducts(user, num)
        def res = resFromModel.map(rate => rate.product -> rate.rating).toMap
        res
      case None => throw new IllegalStateException()
    }
  }

  private def tuneHyperParametersWithGridSearch(evaluateMetric: RDD[Rating]=>Double): Unit = {
    if(alsConfigurationOption_.isDefined && trainRatingsRddOption_.isDefined &&
      validationRatingsRddOption_.isDefined){
      matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.numIterations_, alsConfigurationOption_.get.lambda_))
      var currentBestValidationMetricValue = evaluateMetric(validationRatingsRddOption_.get)

      for (rank <- alsConfigurationOption_.get.ranksList_;
           lambda <- alsConfigurationOption_.get.lambdasList_;
           numberOfIterations <- alsConfigurationOption_.get.numbersOfIterationsList_) {
        matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, rank, numberOfIterations, lambda))
        val validationEvaluateMetric = evaluateMetric(validationRatingsRddOption_.get)
        println("RMSE (validation) = " + validationEvaluateMetric + " for the model trained with rank = "
          + rank + ", lambda = " + lambda + ", and numIter = " + numberOfIterations + ".")
        if (validationEvaluateMetric < currentBestValidationMetricValue) {
          currentBestValidationMetricValue = validationEvaluateMetric
          alsConfigurationOption_ = Some(alsConfigurationOption_.get.copy(rank_ = rank,
            numIterations_ = numberOfIterations, lambda_ = lambda ))
        }
      }
      println("The best model was trained with rank = " + alsConfigurationOption_.get.rank_ +
        " and lambda = " + alsConfigurationOption_.get.lambda_ + ", and numIter = " +
        alsConfigurationOption_.get.numIterations_ + ", and its RMSE on the test set is "
        + currentBestValidationMetricValue + ".")
      SparkAlsPropertiesLoader.saveToDisk(configurationPath_, alsConfigurationOption_.get)
    }
    else{
      throw new IllegalStateException()
    }
  }

  // tuning only number of factors and lambda
  private def tuneHyperParametersWithRandomSearch(evaluateMetric: RDD[Rating]=>Double,
                                                  numberOfIterations:Int, radius:Double): Unit = {
    if(alsConfigurationOption_.isDefined && trainRatingsRddOption_.isDefined &&
      validationRatingsRddOption_.isDefined){
      matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.numIterations_, alsConfigurationOption_.get.lambda_))
      var currentBestValidationMetricValue = evaluateMetric(validationRatingsRddOption_.get)

      for (i <- 1 to numberOfIterations){
        val nextPosition = chooseNextWithAngle((alsConfigurationOption_.get.rank_,
          alsConfigurationOption_.get.lambda_), radius)
        matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, nextPosition._1,
          alsConfigurationOption_.get.numIterations_, nextPosition._2))
        val validationEvaluateMetric = evaluateMetric(validationRatingsRddOption_.get)
        println("RMSE (validation) = " + validationEvaluateMetric + " for the model trained with rank = "
          + nextPosition._1 + ", lambda = " + nextPosition._2 + ", and numIter = " +
          alsConfigurationOption_.get.numIterations_ + ".")

          if (validationEvaluateMetric < currentBestValidationMetricValue) {
            currentBestValidationMetricValue = validationEvaluateMetric
            alsConfigurationOption_ = Some(alsConfigurationOption_.get.copy(rank_ = nextPosition._1,
              lambda_ = nextPosition._2 ))
          }
      }

      println("The best model was trained with rank = " + alsConfigurationOption_.get.rank_ +
        " and lambda = " + alsConfigurationOption_.get.lambda_ + ", and numIter = " +
        alsConfigurationOption_.get.numIterations_ + ", and its RMSE on the test set is "
        + currentBestValidationMetricValue + ".")
      SparkAlsPropertiesLoader.saveToDisk(configurationPath_, alsConfigurationOption_.get)
    }
    else{
      throw new IllegalStateException()
    }
  }

  private def chooseNextWithAngle(currentPosition: (Int, Double),
                                  radius: Double): (Int, Double) = {
    var x:Double = -1.0
    var y:Double = -1.0
    while(x<1||y<0) {
      val angle = 2 * Math.PI * Random.nextDouble()
      x = Math.cos(angle) * radius
      y = Math.sin(angle) * radius
    }
    (currentPosition._1+Math.round(x).toInt, currentPosition._2+y)
  }

}
