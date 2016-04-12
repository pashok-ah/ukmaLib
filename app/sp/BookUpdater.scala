package sp

import javax.inject.{Inject, Singleton}

import org.apache.spark.rdd.RDD
import services.mongo.{MongoServicesHandler, BooksMongoService}

/**
  * Created by P. Akhmedzianov on 31.03.2016.
  */
@Singleton
class BookUpdater extends SparkRatingsFromMongoHandler
  with java.io.Serializable{
  val RATING_PRECISION = 2

  var inputRatingsRdd_ : Option[RDD[(Int, (Int, Double))]] = None
  var inputBooksRdd_ : Option[RDD[(Int, (Int, Option[Double]))]] = None

  def initialize(): Unit = {
    inputRatingsRdd_ = Some(getRatingsCollectionToRdd(true))
    inputBooksRdd_ = Some(getBooksCollectionToRdd())
  }

  def initializeRatings(): Unit = {
    inputRatingsRdd_ = Some(getRatingsCollectionToRdd(false))
  }

  def updateRateCounts(): Unit = {
    val groupedByKeyCollection = inputRatingsRdd_.get.map {
      case (userId, (bookId, rate)) => (bookId, 1)
    }.reduceByKey((x, y) => x + y).collect()

    groupedByKeyCollection.foreach { bookIdAndNumberRates =>
      MongoServicesHandler.booksMongoService.incerementNumRatesForBookId(bookIdAndNumberRates._1,
        bookIdAndNumberRates._2)
    }
  }



  def updateRateCountsAndGlobalRatings(): Unit = {
    val newRatings = getBookIdNumberRatesAndAverageRdd(inputRatingsRdd_.get)

    val joinedRdd = inputBooksRdd_.get.join(newRatings).map {
      case (bookId, (oldOne, newOne)) => (bookId, mergePairs(oldOne, newOne))
    }.collect()

    joinedRdd.foreach { case (bookId, (count, rate)) =>
      MongoServicesHandler.booksMongoService.updateRatingAndCountById(bookId,
        count, rate)
    }
  }

  def setRateCountsAndGlobalRatings(): Unit = {
    println(inputRatingsRdd_.get.count())
    println("Initialized Ratings")
    val newRatings = getBookIdNumberRatesAndAverageRdd(inputRatingsRdd_.get).map{
      case (bookId, (numberOfRate, averageRate)) => (bookId, (numberOfRate, roundDouble(averageRate)))
    }.collect()

    newRatings.foreach { case (bookId, (count, rate)) =>
      MongoServicesHandler.booksMongoService.updateRatingAndCountById(bookId,
        count, rate)
    }
  }

  def getBookIdNumberRatesAndAverageRdd(input: RDD[(Int, (Int, Double))]): RDD[(Int, (Int, Double))] = {
    input.map {
      case (userId, (bookId, rate)) => (bookId, rate)
    }.groupByKey().map {
      case (bookId, rates) => (bookId, iterableOfRatesToNumberAndAverageTuple(rates))
    }
  }

  def update(): Unit ={
    println("I'm updating")
  }


  def mergePairs(oldOne: (Int, Option[Double]), newOne: (Int, Double)): (Int, Double) = {
    if (oldOne._1 > 0 && oldOne._2.isDefined) {
      val res = (oldOne._2.get + newOne._2) / 2.0
      (oldOne._1 + newOne._1, roundDouble(res))
    }
    else {
      (newOne._1, roundDouble(newOne._2))
    }
  }

  def roundDouble = (toBeRounded: Double) => BigDecimal(toBeRounded).setScale(RATING_PRECISION,
    BigDecimal.RoundingMode.HALF_UP).toDouble

  def iterableOfRatesToNumberAndAverageTuple = (iterableOfRates: Iterable[Double]) => {
    val ratesArray = iterableOfRates.toArray
    (ratesArray.length, ratesArray.sum/ratesArray.length)
  }
}
