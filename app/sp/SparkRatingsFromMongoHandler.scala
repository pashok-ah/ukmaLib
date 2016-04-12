package sp

import com.mongodb.BasicDBList
import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import org.bson.types.ObjectId

/**
  * Created by P. Akhmedzianov on 03.03.2016.
  */
trait SparkRatingsFromMongoHandler {
  val MIN_NUMBER_OF_RATES_FOR_USER = 11
  val MIN_NUMBER_OF_RATES_FOR_BOOK = 11

  val RATINGS_COLLECTION_NAME = "ratings"
  val BOOKS_COLLECTION_NAME = "booksShortened"

  def transformToRatingRdd(inputRdd:RDD[(Int, (Int, Double))]):RDD[Rating]={
    inputRdd.map(inputLine => new Rating(inputLine._1, inputLine._2._1, inputLine._2._2))
  }

  def transformToUserKeyRdd(inputRdd:RDD[Rating]):RDD[(Int, (Int, Double))]={
    inputRdd.map{case Rating(user, product, rate) => (user,(product,rate))}
  }

  def getRatingsCollectionToRdd(isFilteringInput:Boolean): RDD[(Int, (Int, Double))] = {
    val mongoConfiguration = new Configuration()
    mongoConfiguration.set("mongo.input.uri",
      play.Play.application.configuration.getString("mongodb.uri") + "." + RATINGS_COLLECTION_NAME)
    // Create an RDD backed by the MongoDB collection.
    val fromMongoRatingsRdd = SparkCommons.sc.newAPIHadoopRDD(
      mongoConfiguration, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type
    val inputRdd = fromMongoRatingsRdd.map(arg => {
        (arg._2.get("users_id").asInstanceOf[Int], (arg._2.get("books_id").asInstanceOf[Int],
          arg._2.get("rate").asInstanceOf[Double]))
      })
    if(isFilteringInput) {
      // filtering the users with number of rates less than MIN_NUMBER_OF_RATES_FOR_USER
      // after that, filtering books with number of raters less than MIN_NUMBER_OF_RATES_FOR_BOOK
      val filteredInputRdd = filterInputByNumberOfKeyEntriesRdd(
        filterInputByNumberOfKeyEntriesRdd(inputRdd, MIN_NUMBER_OF_RATES_FOR_USER).map {
          case (userId, (bookId, rate)) => (bookId, (userId, rate))
        },
        MIN_NUMBER_OF_RATES_FOR_BOOK).map {
        case (bookId, (userId, rate)) => (userId, (bookId, rate))
      }
      println("Number of rates after filtering:"+filteredInputRdd.count())
      filteredInputRdd
    }
    else{
      inputRdd
    }
  }

  def getBooksCollectionToRdd(): RDD[(Int, (Int, Option[Double]))] = {
    val mongoConfiguration = new Configuration()
    mongoConfiguration.set("mongo.input.uri",
      play.Play.application.configuration.getString("mongodb.uri") + "." + BOOKS_COLLECTION_NAME)
    // Create an RDD backed by the MongoDB collection.
    val booksRdd = SparkCommons.sc.newAPIHadoopRDD(
      mongoConfiguration, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type
    val res = booksRdd.map(arg => {
        val optionRatio = if( arg._2.get("globalRate").isInstanceOf[Double])
          Some(arg._2.get("globalRate").asInstanceOf[Double]) else None
        (arg._2.get("_id").asInstanceOf[Int],
          (arg._2.get("numberOfRates").asInstanceOf[Int],
            optionRatio))
      })
    System.out.println("Read books from Mongo:" + res.count())
    res
  }

  def filterInputByNumberOfKeyEntriesRdd(inputRdd: RDD[(Int, (Int, Double))],
                                         threshold: Int): RDD[(Int, (Int, Double))] = {
    val filteredRatingsRdd = inputRdd.groupByKey().filter { groupedLine =>
      val movieRatePairsArray = groupedLine._2.toArray
      movieRatePairsArray.size >= threshold
    }.flatMapValues(x => x)
    filteredRatingsRdd
  }
}


trait PersonalizedRecommender{
    def predict(user: Int, product: Int): Double

    def recommendProducts(user: Int, num: Int): Map[Int, Double]

}
