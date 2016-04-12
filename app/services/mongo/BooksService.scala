package services.mongo

/**
  * Created by P. Akhmedzianov on 17.03.2016.
  */

import javax.inject.Singleton

import models.Book
import play.api.Play.current
import play.api.libs.json.{JsObject, Json}
import play.modules.reactivemongo.ReactiveMongoApi
import play.modules.reactivemongo.json._
import play.modules.reactivemongo.json.collection._
import reactivemongo.api.DB

import scala.concurrent.Future
import play.api.libs.concurrent.Execution.Implicits.defaultContext


trait BooksService extends CRUDService[Book, Int]

@Singleton
class BooksMongoService() extends MongoCRUDService[Book, Int]
  with BooksService with java.io.Serializable{
  val db:DB = current.injector.instanceOf[ReactiveMongoApi].db
  override val collection: JSONCollection = db.collection("booksShortened")

  def getBookIdByISBN(isbn: String): Future[Option[Int]] = {
    collection.find(Json.obj("isbn" -> isbn)).one[Book].map {
      case Some(book) => book._id
      case None => None
    }
  }

  def getBookById(id: Int): Future[Option[Book]] = {
    collection.find(Json.obj("_id" -> id)).one[Book]
  }

  def getArrayBookByArrayId(idArray :List[Int]):Future[List[Option[Book]]] = {
    val listOfFuture = idArray.map(bookId => getBookById(bookId))
    val futureOfList = Future.sequence(listOfFuture)
    futureOfList
  }

  def updateFieldById(id: Int, modifier: JsObject) = {
    val selector = Json.obj("_id" -> id)
    collection.update(selector, modifier)
  }

  def updateSimilarBooksField(id: Int, subjects: Array[Int]) = {
    val modifier = Json.obj("$set" -> Json.obj("similarBooks" -> subjects))
    updateFieldById(id, modifier)
  }

  def updateYouMayAlsoLikeBooksField(id: Int, subjects: Iterable[Int]) = {
    val modifier = Json.obj("$set" -> Json.obj("youMayAlsoLikeBooks" -> subjects))
    updateFieldById(id, modifier)
  }

  def incerementNumRatesForBookId(bookId: Int, increment: Int): Unit = {
    val modifier = Json.obj("$inc" -> Json.obj("numberOfRates" -> increment))
    updateFieldById(bookId, modifier)
  }

  def updateRatingById(bookId: Int, newRate: Double) {
    val modifier = Json.obj("$set" -> Json.obj("globalRate" -> newRate))
    updateFieldById(bookId, modifier)
  }

  def updateRatingAndCountById(bookId: Int, newCount:Int, newRate: Double) {
    val modifier = Json.obj("$set" -> Json.obj("numberOfRates" -> newCount, "globalRate" -> newRate))
    updateFieldById(bookId, modifier)
  }
  def updateDescriptionById(bookId: Int, description: String) {
    val modifier = Json.obj("$set" -> Json.obj("description" -> description))
    updateFieldById(bookId, modifier)
  }


}
