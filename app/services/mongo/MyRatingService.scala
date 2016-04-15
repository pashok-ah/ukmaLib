package services.mongo

import javax.inject.Singleton

import models.MyRating
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsObject, Json}
import play.modules.reactivemongo.ReactiveMongoApi
import play.modules.reactivemongo.json._
import play.modules.reactivemongo.json.collection._
import reactivemongo.api
import reactivemongo.api.DB
import reactivemongo.api.commands.UpdateWriteResult
import reactivemongo.bson.BSONObjectID

import scala.concurrent.Future

/**
  * Created by P. Akhmedzianov on 18.03.2016.
  */
trait MyRatingsService  extends CRUDService[MyRating, BSONObjectID]

@Singleton
class MyRatingMongoService() extends MongoCRUDService[MyRating, BSONObjectID] with MyRatingsService {
  val db:DB = current.injector.instanceOf[ReactiveMongoApi].db
  override val collection: JSONCollection = db.collection("ratings")

  def getRateValueByUserAndBookIds(userId:Int, bookId:Int):Future[Option[Double]]={
    val selector = Json.obj("users_id" -> userId, "books_id" -> bookId)
    collection.find(selector).one[MyRating].map{
      case Some(myRating) => Some(myRating.rate)
      case _ => None
    }
  }

  def getRateByUserAndBookIds(userId:Int, bookId:Int):Future[Option[MyRating]]={
    if(userId > -1){
      val selector = Json.obj("users_id" -> userId, "books_id" -> bookId)
      collection.find(selector).one[MyRating]
    }
    else{
      Future.successful(None)
    }
  }

  def updateRating(selector:JsObject, newRate:Double):Future[UpdateWriteResult]={
    val modifier = Json.obj("$set" -> Json.obj("rate" -> newRate))
    collection.update(selector, modifier)
  }

  def updateExistingRating(id:BSONObjectID, newRate:Double):Future[UpdateWriteResult]={
    val selector = Json.obj("_id" -> id)
    updateRating(selector, newRate)
  }
  def updateExistingRating(userId:Int, bookId:Int, newRate:Double):Future[UpdateWriteResult]={
    val selector = Json.obj("users_id" -> userId, "books_id" -> bookId )
    updateRating(selector, newRate)
  }

  def removeRatingByUserAndBook (userId:Int, bookId:Int):Future[api.commands.WriteResult] ={
    collection.remove(Json.obj("users_id" -> userId, "books_id" -> bookId ))
  }
}
