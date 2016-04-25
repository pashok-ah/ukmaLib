package spark


import javax.inject.{Inject, Singleton}

import com.mongodb.BasicDBList
import org.apache.spark.rdd.RDD
import org.bson.BSONObject

import scala.collection.mutable


/**
  * Created by P. Akhmedzianov on 23.03.2016.
  */
@Singleton
class SubjectsSparkRecommender @Inject()(val configuration: play.api.Configuration)
  extends SparkMongoHandler(configuration) with java.io.Serializable {

  val numOfSimilarBooksToFind_ = configuration.getInt(
    "subjectsSparkRecommender.numberOfBooksToStore").getOrElse(5)

  private def getIdAndSubjectsRdd(inputRdd: RDD[(Object, BSONObject)]): RDD[(Int, Array[String])] = {
    val res = inputRdd.map(arg => {
      val arrFeatures = arg._2.get("subjects").asInstanceOf[BasicDBList].toArray
      (arg._2.get("_id").asInstanceOf[Int], arrFeatures.map(_.toString))
    })
    res
  }

  def updateSimilarBooks(): Unit = {
    val inputRDD = getIdAndSubjectsRdd(getCollectionFromMongoRdd(booksCollectionName_))
      .mapValues {
        case arrayFeatures => arrayFeatures.filter((feature) => feature != "Fiction")
      }
    //getting (bookid,numSubjects),Array[String]
    val inputWithNumSubjectsRDD = inputRDD.map {
      case (bookId, featuresArray) => ((bookId, featuresArray.size), featuresArray)
    }
    //got featureName, (bookid, numFeaturesForBook, numBooksWithFeature)
    val joinedWithFeatureCountsRdd = inputWithNumSubjectsRDD.flatMapValues(x => x)
      .map {
        case ((bookId, numberOfFeatures), featureName) => (featureName, (bookId, numberOfFeatures))
      }
    //creating pairs with common features ((book1Id, book2Id),(numFeatures1Book, numFeatures2Book, numBoooksWithfeature)
    val bookPairsRDD = joinedWithFeatureCountsRdd.join(joinedWithFeatureCountsRdd)
      .filter(line => line._2._1._1 < line._2._2._1)
      .map { case (featureName, ((bookId1, numberOfFeatures1),
      (bookId2, numberOfFeatures2))) =>
        ((bookId1, bookId2), (1, numberOfFeatures1, numberOfFeatures2))
      }
    joinedWithFeatureCountsRdd.unpersist()
    // getting (book1Id,book2Id), Option[Correlation:Double]
    val joinedBooksRDD = bookPairsRDD
      .reduceByKey((left, right) => (left._1 + right._1, left._2, left._3))
      .mapValues(value => calculateCorrelation(value))
    bookPairsRDD.unpersist()
    //getting bookid, Array[bookid]
    val similarBooksIdsRdd = joinedBooksRDD.map { case ((book1Id, book2Id), optionCorrelation) =>
      Array((book1Id, (book2Id, optionCorrelation)), (book2Id, (book1Id, optionCorrelation)))
    }
      .flatMap(x => x)
      .aggregateByKey(mutable.HashSet.empty[(Int, Option[Double])])(addToSet, mergePartitionSets)
      .map { case (bookId, similarBooksSet) =>
        val arrayOfSimilarBooks = similarBooksSet.toArray.sortBy(_._2).reverse.map(line => line._1)
        if (arrayOfSimilarBooks.length >= numOfSimilarBooksToFind_)
          (bookId, arrayOfSimilarBooks.take(numOfSimilarBooksToFind_))
        else (bookId, arrayOfSimilarBooks)
      }
    joinedBooksRDD.unpersist()
    updateMongoCollectionWithRdd(booksCollectionName_, similarBooksIdsRdd.map { resTuple =>
      (new Object, getMongoUpdateWritableFromIdValueTuple[Int, Array[Int]](resTuple, "_id", "similarBooks"))
    })
    similarBooksIdsRdd.unpersist()
  }

  private def calculateCorrelation(input: (Int, Int, Int)): Option[Double] = {
    val numCommonFeatures = input._1
    val num1Features = input._2
    val num2Features = input._3

    val numerator = numCommonFeatures
    val denominator = num1Features + num2Features
    Some(numerator.toDouble / denominator.toDouble)

  }
}
