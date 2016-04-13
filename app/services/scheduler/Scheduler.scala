package services.scheduler

import javax.inject.{Inject, Named}

import akka.actor.{ActorRef, ActorSystem}
import play.api.Configuration

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created by P. Akhmedzianov on 09.04.2016.
  */
class Scheduler @Inject()(val system: ActorSystem, @Named("scheduler-actor") val schedulerActor: ActorRef,
                           val configuration: Configuration)(implicit ec: ExecutionContext)
{
  val updateRatingsInitial = configMilliseconds("scheduler.updateRatings.initial", 1 day)
  val updateRatingsPeriod = configMilliseconds("scheduler.updateRatings.period", 1 day)

  val updatePersonalRecommendationsInitial = configMilliseconds("sheduler.updatePersonalRecommendations.initial",
    1 day)
  val updatePersonalRecommendationsPeriod = configMilliseconds("sheduler.updatePersonalRecommendations.period",
    1 day)

  val updateYouMayAlsoLikeBooksInitial = configMilliseconds("sheduler.updateYouMayAlsoLikeBooks.initial", 1 day)
  val updateYouMayAlsoLikeBooksPeriod = configMilliseconds("sheduler.updateYouMayAlsoLikeBooks.period", 1 day)

  system.scheduler.schedule(updatePersonalRecommendationsInitial, updatePersonalRecommendationsPeriod,
    schedulerActor, "updatePersonalRecommendations")
  system.scheduler.schedule( updateRatingsInitial, updateRatingsPeriod,
    schedulerActor, "updateRatings")
  system.scheduler.schedule( updateYouMayAlsoLikeBooksInitial, updateYouMayAlsoLikeBooksPeriod,
    schedulerActor, "updateYouMayAlsoLikeBooks")
  system.scheduler.schedule( 30 days, 30 days,
    schedulerActor, "updateSimilarBooks")

  def configMilliseconds(key: String, default: FiniteDuration): FiniteDuration =
    configuration.getMilliseconds(key).map(_.milliseconds).getOrElse(default)
}
