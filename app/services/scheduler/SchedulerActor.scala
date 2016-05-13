package services.scheduler

import javax.inject.{Inject, Singleton}

import akka.actor.Actor
import spark._

/**
  * Created by P. Akhmedzianov on 09.04.2016.
  */
@Singleton
class SchedulerActor @Inject() (bookUpdater :  BookGlobalRatingsUpdater,
                                mlLibAlsSparkRatingsFromMongoHandler:  MlLibAlsSparkRatingsRecommender,
                                contentBasedSparkRatingsRecommender:  ContentBasedSparkRatingsRecommender,
                                hybrid:  HybridNearestNeighboursRecommender,
                                subjectsSparkRecommender: SubjectsSparkRecommender)
  extends Actor {
  def receive = {
    case "updateRatings" => updateRatingsInDb()
    case "updatePersonalRecommendations" => updatePersonalReommendationsInDb()
    case "updateYouMayAlsoLikeBooks" => updateYouMayAlsoLikeBooks()
    case "updateSimilarBooks" => updateSimilarBooks()
  }

  def updateRatingsInDb(): Unit ={
    println("Updating...")
    bookUpdater.setRateCountsAndGlobalRatings()
  }

  def updatePersonalReommendationsInDb(): Unit ={
    println("Updating personal recommendations...")
    hybrid.test()
  }

  def updateYouMayAlsoLikeBooks(): Unit ={
    println("Updating YouMayAlsoLike books...")
    contentBasedSparkRatingsRecommender.updateYouMayAlsoLikeBooks()
  }


  def updateSimilarBooks(): Unit ={
    println("Updating similar books...")
    subjectsSparkRecommender.updateSimilarBooks()
  }



}