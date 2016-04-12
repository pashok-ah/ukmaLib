package services.scheduler

import javax.inject.{Inject, Singleton}

import akka.actor.Actor
import sp.{ContentBasedSparkRatingsRecommender, SubjectsSparkRecommender, MlLibAlsSparkRatingsFromMongoHandler, BookUpdater}

/**
  * Created by P. Akhmedzianov on 09.04.2016.
  */
@Singleton
class SchedulerActor @Inject() (bookUpdater : BookUpdater,
                                mlLibAlsSparkRatingsFromMongoHandler: MlLibAlsSparkRatingsFromMongoHandler,
                                contentBasedSparkRatingsRecommender: ContentBasedSparkRatingsRecommender)
  extends Actor {
  def receive = {
    case "updateRatings" => updateRatingsInDb()
    case "updatePersonalRecommendations" => updatePersonalReommendationsInDb()
    case "updateYouMayAlsoLikeBooks" => updateYouMayAlsoLikeBooks()
  }

  def updateRatingsInDb(): Unit ={
    println("Updating...")
    bookUpdater.initializeRatings()
    bookUpdater.setRateCountsAndGlobalRatings()
  }

  def updatePersonalReommendationsInDb(): Unit ={
    println("Updating personal recommendations...")
    mlLibAlsSparkRatingsFromMongoHandler.updateRecommendationsInMongo (isTuning = false)
  }

  def updateYouMayAlsoLikeBooks(): Unit ={
    println("Updating similar books...")
    contentBasedSparkRatingsRecommender.updateYouMayAlsoLikeBooks()
  }

}