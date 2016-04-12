package services.mongo

import models.Counter
import models.JsonFormats.counterFormat
import play.api.Play._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json
import play.modules.reactivemongo.ReactiveMongoApi
import play.modules.reactivemongo.json._
import play.modules.reactivemongo.json.collection.JSONCollection

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try
/**
  * Created by P. Akhmedzianov on 16.03.2016.
  */
object CounterHandler {
  lazy val reactiveMongoApi = current.injector.instanceOf[ReactiveMongoApi]
  def counters(): JSONCollection = reactiveMongoApi.db.collection[JSONCollection]("counters")

  def getNextSequenceUser(idName:String): Try[Option[Counter]] = {
    val selector = Json.obj("_id" -> idName)
    def updateOp = counters.updateModifier(Json.obj("$inc" -> Json.obj("seq" -> 1)))
    val futureRes: Future[Option[Counter]] = counters.findAndModify(selector, updateOp).map(_.result[Counter])
    Await.ready(futureRes, Duration.Inf).value.get
  }

}
