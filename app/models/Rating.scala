package models

import models.Identity
import org.apache.spark.mllib.recommendation.Rating
import play.api.libs.json._
import reactivemongo.bson.BSONObjectID

/**
  * Created by P. Akhmedzianov on 18.02.2016.
  */
case class MyRating(_id:Option[BSONObjectID], users_id:Int, books_id:Int, rate:Double)

object MyRating {
  import play.api.libs.json.Json
  import play.api.data._
  import play.api.data.Forms._
  import play.modules.reactivemongo.json.BSONFormats._
  implicit def myRating2Rating(value : MyRating) = new Rating(value.users_id,value.books_id,value.rate)

implicit val myRatingFormat = Json.format[MyRating]

  implicit object MyRatingIdentity extends Identity[MyRating, BSONObjectID] {
    val name = "_id"
    def of(entity: MyRating): Option[BSONObjectID] = entity._id
    def set(entity: MyRating, id: BSONObjectID): MyRating = entity.copy(_id = Some(id))
    def clear(entity: MyRating): MyRating = entity.copy(_id = None)
    def next: BSONObjectID = BSONObjectID.generate
  }
}
