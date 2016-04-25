package spark

import javax.inject.{Inject, Singleton}

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import play.api.Configuration

import scala.collection.mutable

/**
  * Created by P. Akhmedzianov on 03.03.2016.
  */
@Singleton
class MlLibAlsSparkRatingsRecommender @Inject()(val configuration: Configuration)
  extends SparkMongoHandler(configuration) with java.io.Serializable{

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
      if (isTuningParameters) tuneHyperParametersWithGridSearch()
      initializeModelWithRdd(trainRatingsRddOption_.get)
    }
  }

  def updateRecommendationsInMongo(isTuning : Boolean): Unit ={
    initialize(isTuning, isTuning, isInitializingTestRdd = false)
    exportAllPredictionsToMongo(numberOfRecommendedBooks_)
  }

  private def tuneHyperParametersWithGridSearch(): Unit = {
    if(alsConfigurationOption_.isDefined && trainRatingsRddOption_.isDefined &&
      validationRatingsRddOption_.isDefined){
      matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, alsConfigurationOption_.get.rank_,
        alsConfigurationOption_.get.numIterations_, alsConfigurationOption_.get.lambda_))
      var currentBestValidationRmse = getRmseForRdd(validationRatingsRddOption_.get)

      for (rank <- alsConfigurationOption_.get.ranksList_;
           lambda <- alsConfigurationOption_.get.lambdasList_;
           numberOfIterations <- alsConfigurationOption_.get.numbersOfIterationsList_) {
        matrixFactorizationModelOption_ = Some(ALS.train(trainRatingsRddOption_.get, rank, numberOfIterations, lambda))
        val validationRmse = getRmseForRdd(validationRatingsRddOption_.get)
        println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
          + rank + ", lambda = " + lambda + ", and numIter = " + numberOfIterations + ".")
        if (validationRmse < currentBestValidationRmse) {
          currentBestValidationRmse = validationRmse
          alsConfigurationOption_ = Some(alsConfigurationOption_.get.copy(rank_ = rank,
             numIterations_ = numberOfIterations, lambda_ = lambda ))
        }
      }
      println("The best model was trained with rank = " + alsConfigurationOption_.get.rank_ +
        " and lambda = " + alsConfigurationOption_.get.lambda_ + ", and numIter = " +
        alsConfigurationOption_.get.numIterations_ + ", and its RMSE on the test set is "
        + currentBestValidationRmse + ".")
      SparkAlsPropertiesLoader.saveToDisk(configurationPath_, alsConfigurationOption_.get)
    }
    else{
      throw new IllegalStateException()
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


  protected def getRmseForRdd(ratings: RDD[Rating]): Double = {
    // Evaluate the model on rating data
      val usersProducts = ratings.map { case Rating(user, product, rate) =>
        (user, product)
      }
      val predictions =
        predict(usersProducts).map { case Rating(user, product, rate) =>
          ((user, product), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.join(predictions)
      val meanSquaredError = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
        val err = r1 - r2
        err * err
      }.mean()
      Math.sqrt(meanSquaredError)
  }

  def predict(userProducts:RDD[(Int, Int)]):RDD[Rating]={
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

  protected def getUserFeaturesRdd():RDD[(Int, Array[Double])]={
    matrixFactorizationModelOption_.get.userFeatures
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
        def res = resFromModel.map(rate => (rate.product -> rate.rating)).toMap
        res
      case None => throw new IllegalStateException()
    }
  }
}
