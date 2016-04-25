package spark

import com.mongodb.BasicDBObject
import com.mongodb.hadoop.io.MongoUpdateWritable
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.bson.BSONObject

import scala.collection.mutable

/**
  * Created by P. Akhmedzianov on 03.03.2016.
  */
abstract class SparkMongoHandler(configuration: play.api.Configuration)
  extends java.io.Serializable {
  val ratingsCollectionName_ = configuration.getString("mongodb.ratingsCollectionName")
    .getOrElse("ratings")
  val usersCollectionName_ = configuration.getString("mongodb.usersCollectionName")
    .getOrElse("users")
  val booksCollectionName_ = configuration.getString("mongodb.booksCollectionName")
    .getOrElse("booksShortened")

  val mongoDbUri_ = configuration.getString("mongodb.uri")
    .getOrElse("mongodb://localhost:27017/BookRecommenderDB")

  def transformToRatingRdd(inputRdd: RDD[(Int, (Int, Double))]): RDD[Rating] = {
    inputRdd.map(inputLine => new Rating(inputLine._1, inputLine._2._1, inputLine._2._2))
  }

  def transformToUserKeyRdd(inputRdd: RDD[Rating]): RDD[(Int, (Int, Double))] = {
    inputRdd.map { case Rating(user, product, rate) => (user, (product, rate)) }
  }

  def getCollectionFromMongoRdd(inputCollectionName: String):
  RDD[(Object, BSONObject)] = {
    val mongoConfiguration = new Configuration()
    mongoConfiguration.set("mongo.input.uri", mongoDbUri_ + "." + inputCollectionName)
    // Create an RDD backed by the MongoDB collection.
    val fromMongoRdd = SparkCommons.sc.newAPIHadoopRDD(
      mongoConfiguration, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type
    System.out.println("Read from Mongo:" + fromMongoRdd.count())
    fromMongoRdd
  }

  def updateMongoCollectionWithRdd(updateCollectionName: String,
                                   toMongoUpdateRdd: RDD[(Object, MongoUpdateWritable)]): Unit = {
    val outputConfig = new Configuration()
    outputConfig.set("mongo.output.uri", mongoDbUri_ + "." + updateCollectionName)
    toMongoUpdateRdd.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[MongoUpdateWritable],
      classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
      outputConfig)
  }

  def getKeyValueRatings(inputRdd: RDD[(Object, BSONObject)]):
  RDD[(Int, (Int, Double))] = {
    val res = inputRdd.map(arg => {
      (arg._2.get("users_id").asInstanceOf[Int], (arg._2.get("books_id").asInstanceOf[Int],
        arg._2.get("rate").asInstanceOf[Double]))
    })
    res
  }

  def getMongoUpdateWritableFromIdValueTuple[ID, VAL](tuple: (ID, VAL), idFieldName: String,
                                                      valueFieldName: String): MongoUpdateWritable = {
    new MongoUpdateWritable(
      new BasicDBObject(idFieldName, tuple._1), // Query
      new BasicDBObject("$set", new BasicDBObject(valueFieldName, tuple._2)), // Update operation
      false, // Upsert
      false // Update multiple documents
    )
  }

  def addToSet[T](s: mutable.HashSet[T], v: T): mutable.HashSet[T] = s += v

  def mergePartitionSets[T](p1: mutable.HashSet[T],
                            p2: mutable.HashSet[T]): mutable.HashSet[T] = p1 ++= p2
}



