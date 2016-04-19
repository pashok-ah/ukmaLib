package spark


import javax.inject.{Inject, Singleton}

import com.mongodb.BasicDBList
import org.apache.spark.rdd.RDD
import org.bson.BSONObject


/**
  * Created by P. Akhmedzianov on 23.03.2016.
  */
@Singleton
class SubjectsSparkRecommender @Inject()(val configuration: play.api.Configuration)
  extends SparkMongoHandler(configuration) with java.io.Serializable {

  val numOfSimilarBooksToFind_ = configuration.getInt(
    "subjectsSparkRecommender.numberOfBooksToStore").getOrElse(5)

  private def getIdAndSubjectsRdd(inputRdd: RDD[(Object, BSONObject)]):RDD[(Int, Array[String])]={
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
    //get distinct features and number of books with this feature
    val distinctFeaturesAndCountersRdd = calculateDistinctFeaturesCounts(inputRDD)
    //got featureName, (bookid, numFeaturesForBook, numBooksWithFeature)
    val joinedWithFeatureCountsRdd = inputWithNumSubjectsRDD.flatMapValues(x => x)
      .map {
        case ((bookId, numberOfFeatures), featureName) => (featureName, (bookId, numberOfFeatures))
      }
      .join(distinctFeaturesAndCountersRdd)
      .map {
        case (featureName, ((bookId, numberOfFeatures), featureCollectionCount)) =>
          (featureName, (bookId, numberOfFeatures, featureCollectionCount))
      }
    //creating pairs with common features ((book1Id, book2Id),(numFeatures1Book, numFeatures2Book, numBoooksWithfeature)
    val bookPairsRDD = joinedWithFeatureCountsRdd.join(joinedWithFeatureCountsRdd)
      .filter(line => line._2._1._1 < line._2._2._1)
      .map { case (featureName, ((bookId1, numberOfFeatures1, featureCollectionCount1),
      (bookId2, numberOfFeatures2, featureCollectionCount2))) =>
        ((bookId1, bookId2), (numberOfFeatures1, numberOfFeatures2, featureCollectionCount1))
      }
    joinedWithFeatureCountsRdd.unpersist()
    // getting (book1Id,book2Id), Option[Correlation:Double]
    val joinedBooksRDD = bookPairsRDD.mapValues(featureNumbers => List(featureNumbers))
      .reduceByKey((left, right) => left.++(right))
      .mapValues(value => calculateCorrelation(value))
    bookPairsRDD.unpersist()
    //getting bookid, Array[bookid]
    val similarBooksIdsRdd = joinedBooksRDD.map { case ((book1Id, book2Id), optionCorrelation) =>
      Array((book1Id, (book2Id, optionCorrelation)), (book2Id, (book1Id, optionCorrelation)))
    }
      .flatMap(x => x)
      .map { ln => (ln._1, List(ln._2)) }
      .reduceByKey((left, right) => left.++(right))
      .map { case (bookId, similarBooksList) =>
        val arrayOfSimilarBooks = similarBooksList.toArray.sortBy(_._2).reverse.map(line => line._1)
        if (arrayOfSimilarBooks.length >= numOfSimilarBooksToFind_)
          (bookId, arrayOfSimilarBooks.take(numOfSimilarBooksToFind_))
        else (bookId, arrayOfSimilarBooks)
      }
    joinedBooksRDD.unpersist()
    updateMongoCollectionWithRdd(booksCollectionName_,
      similarBooksIdsRdd.map { resTuple =>
        (new Object, getMongoUpdateWritableFromIdValueTuple[Int, Array[Int]](resTuple, "_id", "similarBooks"))
      })
    similarBooksIdsRdd.take(5).foreach(x => println(x._1 + " and recs:  " + x._2.mkString(" | ")))
    similarBooksIdsRdd.unpersist()
  }

  //getting distinct features and number of books with this feature rdd
  private def calculateDistinctFeaturesCounts(input: RDD[(Int, Array[String])]): RDD[(String, Int)] = {
    val newRdd = input.map(inp => inp._2)
      .flatMap(str => str).map(s => (s, 1))
      .reduceByKey((a, b) => a + b)
      .filter(_._2 >= 2)
    println("Distinct features:" + newRdd.count())
    newRdd
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
      Some(numerator.toDouble / denominator.toDouble)
    }
    else {
      throw new Exception("Feature array size = 0!")
    }
  }
}
