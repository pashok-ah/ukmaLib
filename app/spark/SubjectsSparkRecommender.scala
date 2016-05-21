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

  private def getTitleIdAndSubjectsRdd(inputRdd: RDD[(Object, BSONObject)]):
  RDD[(String, (Int, Array[String]))] = {
    val res = inputRdd.map(arg => {
      val arrFeatures = arg._2.get("subjects").asInstanceOf[BasicDBList].toArray
      (arg._2.get("bookTitle").asInstanceOf[String].toLowerCase, (arg._2.get("_id").asInstanceOf[Int],
        arrFeatures.map(_.toString)))
    })
    res
  }

  // groping id's by Title
  def filter(inputRdd: RDD[(String, (Int, Array[String]))]): RDD[((Int, Array[Int]),Array[String])] = {
    val distinctNamesRdd = inputRdd
      .aggregateByKey(mutable.HashSet.empty[(Int, Array[String])])(addToSet, mergePartitionSets)
      .map { case (bookTitle, hashSet) =>
        val arrayOfIds = hashSet.toArray.map(_._1)
        ((arrayOfIds.head, arrayOfIds.distinct),hashSet.head._2) }
    println("Count after removing similar titles:"+distinctNamesRdd.count())
    distinctNamesRdd
  }

  // using title as key coz of different editions of the same book
  def updateSimilarBooks(): Unit = {
    val inputRDD = filter(getTitleIdAndSubjectsRdd(getCollectionFromMongoRdd(booksCollectionName_)))
      .mapValues {
        case arrayFeatures => arrayFeatures.filter((feature) => feature != "Fiction")
      }

    val inputWithNumSubjectsRDD = inputRDD.map {
      case ((bookId,idArray), featuresArray) => ((bookId, idArray, featuresArray.length), featuresArray)
    }

    val joinedWithFeatureCountsRdd = inputWithNumSubjectsRDD.flatMapValues(x => x)
      .map {
        case ((bookId, idArray, numberOfFeatures), featureName) => (featureName, (bookId, idArray, numberOfFeatures))
      }

    val bookPairsRDD = joinedWithFeatureCountsRdd.join(joinedWithFeatureCountsRdd)
      .filter(line => line._2._1._1 < line._2._2._1)
      .map { case (featureName, ((bookId1, idArray1, numberOfFeatures1),
      (bookId2, idArray2, numberOfFeatures2))) =>
        ((bookId1, bookId2), (1, idArray1, idArray2, numberOfFeatures1, numberOfFeatures2))
      }
    joinedWithFeatureCountsRdd.unpersist()
    // getting (book1Id,book2Id), Option[Correlation:Double]
    val joinedBooksRDD = bookPairsRDD
      .reduceByKey((left, right) => (left._1 + right._1, left._2, left._3, left._4, left._5))
        .map{case ((book1Id, book2Id),
        (commonFeaturesNumber, idArray1, idArray2, numberOfFeatures1, numberOfFeatures2)) =>
          ((book1Id, book2Id,idArray1, idArray2), (commonFeaturesNumber, numberOfFeatures1, numberOfFeatures2))
        }
      .mapValues(value => calculateCorrelation(value))
    bookPairsRDD.unpersist()
    //getting bookid, Array[bookid]
    val similarBooksIdsRdd = joinedBooksRDD.map {
      case ((book1Id, book2Id, idArray1, idArray2), optionCorrelation) =>
      Array((book1Id, (book2Id, optionCorrelation, idArray1)), (book2Id, (book1Id, optionCorrelation, idArray2)))
    }
      .flatMap(x => x)
      .aggregateByKey(mutable.HashSet.empty[(Int, Option[Double], Array[Int])])(addToSet, mergePartitionSets)
      .map { case (bookId, similarBooksSet) =>
        var arrayOfSimilarBooks = similarBooksSet.toArray.sortBy(_._2).reverse.map(line => line._1)
        arrayOfSimilarBooks = if (arrayOfSimilarBooks.length > numOfSimilarBooksToFind_)
          arrayOfSimilarBooks.take(numOfSimilarBooksToFind_) else arrayOfSimilarBooks
        val fullArrayIdsOfBooksWithSameName = similarBooksSet.head._3.map{
          case bookID => (bookID, arrayOfSimilarBooks)
        }
        fullArrayIdsOfBooksWithSameName
      }.flatMap (x => x)
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
