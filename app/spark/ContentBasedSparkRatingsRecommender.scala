package spark

import javax.inject.{Inject, Singleton}

/**
  * Created by P. Akhmedzianov on 24.03.2016.
  */
@Singleton
class ContentBasedSparkRatingsRecommender @Inject()(val configuration: play.api.Configuration)
  extends SparkRatingsFromMongoHandler with java.io.Serializable {
  val minNumRatesForUserToFilter = 10
  val numOfSimilarBooksToFind_ = configuration.getInt(
    "contentBasedSparkRatingsRecommender.numberOfBooksToStore").getOrElse(5)

  val booksCollectionName_ = configuration.getString("mongodb.booksCollectionName")
    .getOrElse(BOOKS_DEFAULT_COLLECTION_NAME)
  val ratingsCollectionName_ = configuration.getString("mongodb.ratingsCollectionName")
    .getOrElse(RATINGS_DEFAULT_COLLECTION_NAME)

  def updateYouMayAlsoLikeBooks(): Unit = {
    //getting: bookid, List[(uesrid, rating, number of raters)]
    val booksGroupedRdd = getKeyValueRatings(getCollectionFromMongoRdd(ratingsCollectionName_))
      .map { case (userId, (bookId, rate)) =>
        (bookId, (userId, rate))}
      .mapValues(userRatingTuple => List(userRatingTuple))
      .reduceByKey((left, right) => left.++(right))
      .map { case (bookId, listOfTuples) =>
        (bookId, listOfTuples.map(userRatePair => (userRatePair._1, userRatePair._2, listOfTuples.length)))
      }

    //getting: userid (bookid, rating, number of raters)
    val usersRdd = booksGroupedRdd
      .flatMapValues(value => value)
      .map { case (bookId, (userId, rate, numberOfRaters)) =>
        (userId, (bookId, rate, numberOfRaters))
      }
    //getting: ((book1Id,book2Id), List(...))
    val bookPairsGrouped = usersRdd.join(usersRdd)
      .filter(line => line._2._1._1 < line._2._2._1)
      .map { line =>
        val book1 = line._2._1._1
        val book2 = line._2._2._1
        val book1Rating = line._2._1._2
        val book2Rating = line._2._2._2
        val book1Count = line._2._1._3
        val book2Count = line._2._2._3
        ((book1, book2), (book1Rating, book1Count, book2Rating, book2Count, book1Rating * book2Rating,
          book1Rating * book1Rating, book2Rating * book2Rating))
      }
      .mapValues(bigTuple => List(bigTuple)).reduceByKey((left, right) => left.++(right)).filter(_._2.size > 1)

    val resRDD = bookPairsGrouped
      .mapValues(value => calculateCorrelations(value))
      .flatMapValues(x => x)

    val finalRes = resRDD.map { case ((book1Id, book2Id), correlationValue) =>
      Array((book1Id, (book2Id, correlationValue)), (book2Id, (book1Id, correlationValue)))}
      .flatMap(x => x).map { ln => (ln._1, List(ln._2)) }.reduceByKey((left, right) => left.++(right))
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

  private def calculateCorrelations(input: Iterable[(Double, Int, Double, Int, Double, Double, Double)]):
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
