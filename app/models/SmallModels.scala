package models

import models.Identity
import play.api.libs.json.{JsObject, Writes, Json, Reads}
import securesocial.core.providers.MailToken

import scala.util.{Failure, Success}

/**
  * Created by P. Akhmedzianov on 14.02.2016.
  */

case class Counter(_id:String, seq: Long)

object JsonFormats {
  import play.api.libs.json.Json
  import play.api.data._
  import play.api.data.Forms._
  import play.modules.reactivemongo.json.BSONFormats._
  // Generates Writes and Reads for MailToken thanks to Json Macros
  implicit val mailTokenFormat = Json.format[MailToken]
  implicit val counterFormat = Json.format[Counter]
  implicit val bookFormat = Json.format[Book]
}