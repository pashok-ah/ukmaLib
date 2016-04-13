package spark


import com.mongodb.BasicDBObject
import com.mongodb.hadoop.io.MongoUpdateWritable
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
/**
  * Created by P. Akhmedzianov on 03.03.2016.
  */
trait SparkRatingsFromMongoHandler {
  val MIN_NUMBER_OF_RATES_FOR_USER = 11
  val MIN_NUMBER_OF_RATES_FOR_BOOK = 11

  val RATINGS_DEFAULT_COLLECTION_NAME = "ratings"
  val BOOKS_DEFAULT_COLLECTION_NAME = "booksShortened"
  val USERS_DEFAULT_COLLECTION_NAME = "users"

  val defaultMongoDbUri = "mongodb://localhost:27017/BookRecommenderDB"

  def transformToRatingRdd(inputRdd:RDD[(Int, (Int, Double))]):RDD[Rating]={
    inputRdd.map(inputLine => new Rating(inputLine._1, inputLine._2._1, inputLine._2._2))
  }

  def transformToUserKeyRdd(inputRdd:RDD[Rating]):RDD[(Int, (Int, Double))]={
    inputRdd.map{case Rating(user, product, rate) => (user,(product,rate))}
  }

  def getCollectionFromMongoRdd(inputCollectionName: String):
  RDD[(Object, BSONObject)] = {
    val mongoConfiguration = new Configuration()
    mongoConfiguration.set("mongo.input.uri",
      play.Play.application.configuration.getString("mongodb.uri") + "." + inputCollectionName)
    // Create an RDD backed by the MongoDB collection.
    val fromMongoRdd = SparkCommons.sc.newAPIHadoopRDD(
      mongoConfiguration, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type
    System.out.println("Read from Mongo:" + fromMongoRdd.count())
    fromMongoRdd
  }

  def updateMongoCollectionWithRdd(updateCollectionName:String,
                                   toMongoUpdateRdd:RDD[(Object, MongoUpdateWritable)]): Unit ={
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri",
      play.Play.application.configuration.getString("mongodb.uri") + "." +
        updateCollectionName)
    toMongoUpdateRdd.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[MongoUpdateWritable],
      classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
      outputConfig)
  }

  def getKeyValueRatings(inputRdd: RDD[(Object, BSONObject)]):
  RDD[(Int, (Int, Double))]={
    val res = inputRdd.map(arg => {
      (arg._2.get("users_id").asInstanceOf[Int], (arg._2.get("books_id").asInstanceOf[Int],
        arg._2.get("rate").asInstanceOf[Double]))
    })
    res
  }

  def getMongoUpdateWritableFromIdValueTuple[ID,VAL](tuple:(ID, VAL), idFieldName:String,
                                                     valueFieldName:String):MongoUpdateWritable={
    new MongoUpdateWritable(
      new BasicDBObject(idFieldName, tuple._1),  // Query
      new BasicDBObject("$set", new BasicDBObject(valueFieldName, tuple._2)),  // Update operation
      false,  // Upsert
      false   // Update multiple documents
    )
  }
}


trait PersonalizedRecommender{
    def predict(user: Int, product: Int): Double

    def recommendProducts(user: Int, num: Int): Map[Int, Double]

}
