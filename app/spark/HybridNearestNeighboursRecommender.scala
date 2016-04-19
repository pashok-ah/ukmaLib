package spark

import javax.inject.Inject

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import play.api.Configuration

/**
  * Created by P. Akhmedzianov on 17.04.2016.
  */
class HybridNearestNeighboursRecommender @Inject() (configuration: Configuration)
  extends MlLibAlsSparkRatingsRecommender(configuration) with java.io.Serializable{

  val numberOfUsersToFind_ = 50

  private var neighboursRdd : Option[RDD[(Int, Array[(Int, Double)])]] = None

  override def test(): Unit ={
    initialize(isTuningParameters = false, isInitializingValidationRdd = true,
      isInitializingTestRdd = true)
    getNeighboursRdd()
    println("Train RMSE = " + getRmseForRdd(trainRatingsRddOption_.get))
    println("Validation RMSE = " + getRmseForRdd(validationRatingsRddOption_.get))
    neighboursRdd.get.unpersist()
    trainRatingsRddOption_.get.unpersist()
    validationRatingsRddOption_.get.unpersist()
  }

  def getNeighboursRdd()={

    val userFeaturesRdd = getUserFeaturesRdd()

    val joinedRdd = userFeaturesRdd.cartesian(userFeaturesRdd)
      .filter(tuple => tuple._1._1 < tuple._2._1)
      .map{case ((user1, features1), (user2, features2)) =>
        ((user1, user2), cosineSimilarity(features1, features2))
      }.flatMapValues(x => x)
      .map(x => Array((x._1._1, (x._1._2, x._2) ), (x._1._2, (x._1._1, x._2))))
      .flatMap(x => x)
      .map{case (user1, value) => (user1, Set(value))}
      .reduceByKey{
        case (left, right) => left.++(right)
      }.map { resLine =>
      val arrayOfNeighbours = resLine._2.toArray.sortBy(_._2).reverse.take(numberOfUsersToFind_)
      (resLine._1, arrayOfNeighbours)
      }
    neighboursRdd = Some(joinedRdd.persist())
  }

  override def predict(userId:Int, bookId:Int):Double={
    val broadcastNeighboursVar = SparkCommons.sc.broadcast(getNeighbours(userId))

    val filteredRatings = trainRatingsRddOption_.get.filter{
      case Rating(userId, book, rate) => broadcastNeighboursVar.value.contains(userId) && bookId == book
    }
      .map(_.rating)
     filteredRatings.mean()
  }

  override def predict(userProducts: RDD[(Int, Int)]):RDD[Rating]={
    println("Called override predict")
    val neighbourBookUserRdd = userProducts.join(neighboursRdd.get)
      .map{case (user, (book, neighbours)) => ((user, book), neighbours)}
      .flatMapValues(x => x)
      .map{case ((user, book), (neighbour, similarity)) =>
        ((neighbour, book), (user, similarity))}

    val mapped = getUnitedInputRdd().map{
      case Rating(user, book, rate) => ((user, book), rate)
    }
    val resRdd = neighbourBookUserRdd.join(mapped)
      .map{case ((neighbour, book),((user, similarity), rate)) =>
        ((user, book), Set((similarity, rate)))}
      .reduceByKey((leftSet, rightSet) => leftSet.++(rightSet))
      .map{case ((user, book), setOfRates) => new Rating(user, book, calculatePredictedRating(setOfRates))}
    resRdd
  }

  def getNeighbours(userId:Int):Array[Int] = {
    val sequenceUserNeighbours = neighboursRdd.get.lookup(userId)
    if(sequenceUserNeighbours.size == 1) sequenceUserNeighbours.head.map(_._1)
    else throw new Exception("Wrong number")
  }

  def calculatePredictedRating(setOfSimilaritiesAndRates:Set[(Double, Double)]):Double = {
    val sumOfSimilarities = setOfSimilaritiesAndRates.map(_._1).sum
    val array = setOfSimilaritiesAndRates.map{
      case (similarity, rate) => (similarity/sumOfSimilarities, rate)
    }
    (for{tuple <- array} yield tuple._1*tuple._2).sum
  }

  def cosineSimilarity(x: Array[Double], y: Array[Double]): Option[Double] = {
    require(x.size == y.size)
    val res = dotProduct(x, y)/(magnitude(x) * magnitude(y))
    if (res.isNaN) None
    else Some(res)
  }

  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }

  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }


}
