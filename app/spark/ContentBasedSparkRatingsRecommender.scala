package spark

import javax.inject.{Inject, Singleton}

import scala.collection.mutable

/**
  * Created by P. Akhmedzianov on 24.03.2016.
  */
@Singleton
class ContentBasedSparkRatingsRecommender @Inject()(val configuration: play.api.Configuration)
  extends SparkMongoHandler(configuration) with java.io.Serializable {
  type LongTuple = (Double, Int, Double, Int, Double, Double, Double)

  val numOfSimilarBooksToFind_ = configuration.getInt(
    "contentBasedSparkRatingsRecommender.numberOfBooksToStore").getOrElse(5)

  def updateYouMayAlsoLikeBooks(): Unit = {
    //getting: bookid, List[(uesrid, rating, number of raters)]
    val booksGroupedRdd = getKeyValueRatings(getCollectionFromMongoRdd(ratingsCollectionName_))
      .map { case (userId, (bookId, rate)) =>
        (bookId, (userId, rate))}
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .map { case (bookId, setOfTuples) =>
        (bookId, setOfTuples.map(userRatePair => (userRatePair._1, userRatePair._2, setOfTuples.size)))
      }

    //getting: userid (bookid, rating, number of raters)
    val usersRdd = booksGroupedRdd
      .flatMapValues(value => value)
      .map { case (bookId, (userId, rate, numberOfRaters)) =>
        (userId, (bookId, rate, numberOfRaters))
      }.persist()

    //getting: ((book1Id,book2Id), List(...))
    val bookPairsGrouped = usersRdd.join(usersRdd)
      .filter(line => line._2._1._1 < line._2._2._1)
      .map { case (userId, ((book1, book1Rating, book1Count), (book2, book2Rating, book2Count))) =>
        ((book1, book2), (book1Rating, book1Count, book2Rating, book2Count, book1Rating * book2Rating,
          book1Rating * book1Rating, book2Rating * book2Rating))
      }
      .aggregateByKey(mutable.HashSet.empty[LongTuple])(addToSet, mergePartitionSets)
      .filter(_._2.size > 1)
    usersRdd.unpersist()
    val resRDD = bookPairsGrouped
      .mapValues(value => calculateCorrelations(value))
      .flatMapValues(x => x)

    val finalRes = resRDD.map { case ((book1Id, book2Id), correlationValue) =>
      Array((book1Id, (book2Id, correlationValue)), (book2Id, (book1Id, correlationValue)))}
      .flatMap(x => x)
      .aggregateByKey(mutable.HashSet.empty[(Int, Double)])(addToSet, mergePartitionSets)
      .map { resLine =>
        val arrResults = resLine._2.toArray.sortBy(_._2).reverse
        if (arrResults.length >= numOfSimilarBooksToFind_)
          (resLine._1, arrResults.take(numOfSimilarBooksToFind_).map(line => line._1))
        else (resLine._1, arrResults.map(line => line._1))
      }

    updateMongoCollectionWithRdd(booksCollectionName_,
      finalRes.map { resTuple =>
        (new Object, getMongoUpdateWritableFromIdValueTuple[Int, Array[Int]](resTuple, "_id", "youMayAlsoLikeBooks"))
      })
  }

  private def calculateCorrelations(input: Iterable[LongTuple]):
  Option[Double] = {
    var groupSize = 0
    var rating1Sum = 0.0
    var rating2Sum = 0.0
    var rating1NormSq = 0.0
    var rating2NormSq = 0.0
    var dotProduct = 0.0
    for (tuple <- input) {
      groupSize += 1
      rating1Sum += tuple._1
      rating2Sum += tuple._3
      rating1NormSq += tuple._6
      rating2NormSq += tuple._7
      dotProduct += tuple._5
    }
    /*(calculatePearsonCorrelation(groupSize,dotProduct,rating1Sum,rating2Sum,rating1NormSq, rating2NormSq),*/
    calculateCosineCorrelation(dotProduct, Math.sqrt(rating1NormSq), Math.sqrt(rating2NormSq))
  }

  private def calculatePearsonCorrelation(groupSize: Int, dotProduct: Double, rating1Sum: Double, rating2Sum: Double,
                                          rating1NormSq: Double, rating2NormSq: Double): Option[Double] = {
    val numerator = groupSize * dotProduct - rating1Sum * rating2Sum
    val denominator = Math.sqrt(groupSize * rating1NormSq - rating1Sum * rating1Sum) *
      Math.sqrt(groupSize * rating2NormSq - rating2Sum * rating2Sum)
    val res = numerator / denominator
    if (res.isNaN) None
    else Some(res)
  }

  private def calculateCosineCorrelation(dotProduct: Double, rating1Norm: Double,
                                         rating2Norm: Double): Option[Double] = {
    def res = dotProduct / (rating1Norm * rating2Norm)
    if (!res.isNaN && !res.isInfinity) Some(res)
    else None
  }
}
