package spark

import javax.inject.{Inject, Singleton}

import com.mongodb.BasicDBObject
import com.mongodb.hadoop.io.MongoUpdateWritable
import org.apache.spark.rdd.RDD
import play.api.Configuration


/**
  * Created by P. Akhmedzianov on 31.03.2016.
  */
@Singleton
class BookGlobalRatingsUpdater @Inject() (val configuration: Configuration)
  extends SparkRatingsFromMongoHandler
  with java.io.Serializable{
  val RATING_PRECISION = 3

  val thresholdOfRatesToCalculateGlobalRating = 9

  val booksCollectionName_ = configuration.getString("mongodb.booksCollectionName")
    .getOrElse(BOOKS_DEFAULT_COLLECTION_NAME)
  val ratingsCollectionName_ = configuration.getString("mongodb.ratingsCollectionName")
    .getOrElse(RATINGS_DEFAULT_COLLECTION_NAME)

  def setRateCountsAndGlobalRatings(): Unit = {
    val countsAndAverageRdd = getKeyValueRatings(getCollectionFromMongoRdd(ratingsCollectionName_))
      .map { case(userId, (bookId, rate)) => (bookId, (1, List(rate))) }
      .reduceByKey {
        case ((numberOfRates1, rateList1), (numberOfRates2, rateList2)) =>
          (numberOfRates1 + numberOfRates2, rateList1.++(rateList2))}
      .mapValues{case (countRates, listOfRates) =>
        (countRates,
          if(listOfRates.length > thresholdOfRatesToCalculateGlobalRating)
          calculateGlobalRating(listOfRates)
          else None)
      }
    updateMongoCollectionWithRdd(booksCollectionName_,
      countsAndAverageRdd.map { resTuple =>
        (new Object, getMongoUpdateWritableForBookRatingUpdate(resTuple))
      })
  }

  def getBookIdNumberRatesAndAverageRdd(input: RDD[(Int, (Int, Double))]): RDD[(Int, (Int, Double))] = {
    input.map {
      case (userId, (bookId, rate)) => (bookId, rate)
    }.groupByKey().map {
      case (bookId, rates) => (bookId, iterableOfRatesToNumberAndAverageTuple(rates))
    }
  }

  def getMongoUpdateWritableForBookRatingUpdate(tuple:(Int, (Int, Option[Double]))):
  MongoUpdateWritable={
    val updateBasicDbObject = new BasicDBObject
    updateBasicDbObject.put( "numberOfRates" , int2Integer(tuple._2._1))
    if(tuple._2._2.isDefined) {
      updateBasicDbObject.put( "globalRate", double2Double(tuple._2._2.get))
    }
    new MongoUpdateWritable(
      new BasicDBObject("_id", tuple._1),  // Query
      new BasicDBObject("$set", updateBasicDbObject),  // Update operation
      false,  // Upsert
      false   // Update multiple documents
    )
  }

  def calculateGlobalRating(listOfRates:List[Double]):Option[Double]={
    Some(roundDouble(listOfRates.sum/listOfRates.length))
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
