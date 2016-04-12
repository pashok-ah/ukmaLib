package services.mongo

import javax.inject.Singleton

import models.User
import models.User.{UserReads,UserWrites}

import play.api.Play._
import play.api.libs.json.Json
import play.modules.reactivemongo.ReactiveMongoApi

/**
  * Created by P. Akhmedzianov on 16.03.2016.
  */
trait UsersService extends CRUDService[User, Int]

import play.modules.reactivemongo.json.collection._
import reactivemongo.api.DB

@Singleton
class UsersMongoService ()
  extends MongoCRUDService[User, Int] with UsersService {
  val db:DB = current.injector.instanceOf[ReactiveMongoApi].db
  override val collection: JSONCollection = db.collection("users")


  def updateRecommendations(userId:Int, bookIdsArray:Array[Int])={
    val selector = Json.obj("_id" -> userId)
    val modifier = Json.obj("$set" -> Json.obj("personalRecommendations" -> bookIdsArray))
    updateBySelectorAndModifier(selector,modifier)
  }
}


