package spark


import javax.inject.{Inject, Singleton}

import com.mongodb.BasicDBList
import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
import services.mongo.MongoServicesHandler

/**
  * Created by P. Akhmedzianov on 23.03.2016.
  */
@Singleton
class SubjectsSparkRecommender @Inject() (val configuration: play.api.Configuration)
  extends SparkRatingsFromMongoHandler with java.io.Serializable{
  val numOfSimilarBooksToFind_ = configuration.getInt(
    "subjectsSparkRecommender.numberOfBooksToStore").getOrElse(5)

  def updateSimilarBooks(): Unit = {
    val inputRDD = getBooksCollFromMongoToRdd.mapValues{
      case arrayFeatures => arrayFeatures.filter((feature) => feature != "Fiction")
    }/*.randomSplit(Array(0.95, 0.05)).apply(1)*/
    //getting (bookid,numSubjects),Array[String]
    val inputWithNumSubjectsRDD = inputRDD.map {
        case (bookId, featuresArray) =>
      ((bookId, featuresArray.size), featuresArray)
    }
    //get distinct features and number of books with this feature
    val distinctFeaturesAndCountersRdd = calculateDistinctFeatures(inputRDD)
    //getting featureName,(bookId,numFeatures)
    val featureNameKeyRdd = inputWithNumSubjectsRDD.flatMapValues(x => x).map{
      case ((bookId, numberOfFeatures), featureName)=>
      (featureName, (bookId, numberOfFeatures))}
    //got featureName, (bookid, numFeaturesForBook, numBooksWithFeature)
    val joinedFeaturesRdd = featureNameKeyRdd.join(distinctFeaturesAndCountersRdd).map{
      case (featureName, ((bookId, numberOfFeatures), featureCollectionCount))=>
      (featureName, (bookId, numberOfFeatures, featureCollectionCount))}
    System.out.println("joinedFeaturesRDD count:" + joinedFeaturesRdd.count())
    //creating pairs with common features ((book1Id, book2Id),(numFeatures1Book, numFeatures2Book, numBoooksWithfeature)
    val bookPairsRDD = joinedFeaturesRdd.join(joinedFeaturesRdd).filter(line => line._2._1._1 < line._2._2._1).map {
      case (featureName, ((bookId1, numberOfFeatures1, featureCollectionCount1),
      (bookId2, numberOfFeatures2, featureCollectionCount2))) =>
        ((bookId1, bookId2), (numberOfFeatures1, numberOfFeatures2, featureCollectionCount1))
    }
    System.out.println("bookPairsRDD count:" + bookPairsRDD.count())
    // getting (book1Id,book2Id), Option[Correlation:Double]
    // avoiding groupByKey coz of its inefficiency
    val joinedBooksRDD = bookPairsRDD.mapValues(featureNumbers =>
      List(featureNumbers)).reduceByKey((left,right) => left.++(right)).
      mapValues(value => calculateCorrelation(value))

    System.out.println("Joined books count:" + joinedBooksRDD.count())
    //getting bookid, Array[bookid]
    val resRDD = joinedBooksRDD.map{case ((book1Id, book2Id), optionCorrelation ) =>
      Array((book1Id, (book2Id, optionCorrelation)),(book2Id, (book1Id, optionCorrelation)))}.
      flatMap(x => x).map{ln => (ln._1, List(ln._2))}.reduceByKey((left,right) => left.++(right)).
      map { resLine =>
      val arrResults = resLine._2.toArray.sortBy(_._2).reverse
      if (arrResults.length>=numOfSimilarBooksToFind_)
        (resLine._1,arrResults.take(numOfSimilarBooksToFind_).map(line => line._1))
      else (resLine._1,arrResults.map(line => line._1))
    }
    resRDD.collect().foreach{tuple =>
      MongoServicesHandler.booksMongoService.updateSimilarBooksField(tuple._1,tuple._2)}
  }

  //getting distinct features and number of books with this feature rdd
  private def calculateDistinctFeatures(input: RDD[(Int, Array[String])]): RDD[(String, Int)] = {
    val newRdd = input.map(inp => inp._2).flatMap(str => str).map(s => (s, 1)).reduceByKey((a, b) => a + b).filter(_._2 >= 2)
    println("distinct features after filtering count:"+newRdd.count())
    newRdd
  }

  private def getBooksCollFromMongoToRdd: RDD[(Int, Array[String])] = {
    val mongoConfiguration = new Configuration()
    mongoConfiguration.set("mongo.input.uri",
      play.Play.application.configuration.getString("mongodb.uri") + "." + BOOKS_COLLECTION_NAME)
    // Create an RDD backed by the MongoDB collection.
    val ratingsRdd = SparkCommons.sc.newAPIHadoopRDD(
      mongoConfiguration, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type
    val res = ratingsRdd.map(arg => {
        val arrFeatures = arg._2.get("subjects").asInstanceOf[BasicDBList].toArray
        (arg._2.get("_id").asInstanceOf[Int], arrFeatures.map(_.toString))
      })
    System.out.println("Read from Mongo:" + res.count())
    res
  }

  private def calculateCorrelation(input: Iterable[(Int, Int, Int)]): Option[Double] = {
    val inputArr = input.toArray
    if (inputArr.length > 0) {
      val numCommonFeatures = inputArr.size

      val num1Features = inputArr(0)._1
      val num2Features = inputArr(0)._2
      val sumOfFeatureCollectionFrequencies = inputArr.map(_._3).sum

      val numerator = numCommonFeatures
      val denominator = num1Features + num2Features
      Some((numerator.toDouble/sumOfFeatureCollectionFrequencies.toDouble) / denominator.toDouble)
    }
    else {
      throw new Exception("Feature array size = 0!")
    }
  }
}
