package models

import models.Identity
import play.api.libs.json._
import securesocial.core.{BasicProfile, AuthenticationMethod, PasswordInfo, OAuth2Info}
import services.mongo.CounterHandler

import scala.util.{Failure, Success}

/**
  * Created by P. Akhmedzianov on 31.03.2016.
  */
case class Book(
                 _id : Option[Int],
                 isbn: String,
                 bookTitle: String,
                 bookAuthor: String,
                 yearOfPublication: Short,
                 publisher: String,
                 imageURLSmall: String,
                 imageURLMedium: String,
                 imageURLLarge: String,
                 subjects: Array[String],
                 similarBooks: Array[Int],
                 youMayAlsoLikeBooks:Array[Int],
                 numberOfRates:Int,
                 globalRate: Option[Double],
                 description: Option[String]
               )
object Book {
  implicit object BookWrites extends OWrites[Book] {
    def writes(book: Book): JsObject = Json.obj(
      "_id" -> book._id,
      "isbn" -> book.isbn,
      "bookTitle" -> book.bookTitle,
      "bookAuthor" -> book.bookAuthor,
      "yearOfPublication" -> book.yearOfPublication,
      "publisher" -> book.publisher,
      "imageURLSmall" -> book.imageURLSmall,
      "imageURLMedium" -> book.imageURLMedium,
      "imageURLLarge" -> book.imageURLLarge,
      "subjects" -> book.subjects,
      "similarBooks" -> book.similarBooks,
      "youMayAlsoLikeBooks" -> book.similarBooks,
      "numberOfRates" -> book.numberOfRates,
      "globalRate" -> book.globalRate,
      "description" -> book.description
    )
  }

  implicit object BookReads extends Reads[Book] {
    def reads(json: JsValue): JsResult[Book] = json match {
      case obj: JsObject => try {
        val _id = (json \ "_id").as[Int]
        val isbn = (json \ "isbn").as[String]
        val bookTitle = (json \ "bookTitle").as[String]
        val bookAuthor = (json \ "bookAuthor").as[String]
        val yearOfPublication = (json \ "yearOfPublication").as[Short]
        val publisher = (json \ "publisher").as[String]
        val imageURLSmall = (json \ "imageURLSmall").as[String]
        val imageURLMedium = (json \ "imageURLMedium").as[String]
        val imageURLLarge = (json \ "imageURLLarge").as[String]
        val subjects = (json \ "subjects").as[Array[String]]
        val similarBooks = (json \ "similarBooks").as[Array[Int]]
        val youMayAlsoLikeBooks = (json \ "youMayAlsoLikeBooks").as[Array[Int]]
        val numberOfRates = (json \ "numberOfRates").as[Int]
        val globalRate = (json \ "globalRate").asOpt[Double]
        val description = (json \ "description").asOpt[String]

        val book: Book = new Book(Some(_id), isbn, bookTitle, bookAuthor, yearOfPublication,
          publisher, imageURLSmall, imageURLMedium, imageURLLarge, subjects, similarBooks,
          youMayAlsoLikeBooks,numberOfRates, globalRate, description)
        JsSuccess(book)
      } catch {
        case cause: Throwable => JsError(cause.getMessage)
      }
      case _ => JsError("expected.jsobject")
    }
  }

  implicit object BookIdentity extends Identity[Book, Int]{
    def name: String = "_id"

    override def next: Int =  {
      val res = CounterHandler.getNextSequenceUser("bookid") match {
        case Success(t) => Some(Right(t).b.get.seq.toInt)
        case Failure(e) => null
      }
      res.get
    }
    override def of(entity: Book): Option[Int] = entity._id
    override def set(entity: Book, id: Int): Book = entity.copy(_id = Some(id))
    override def clear(entity: Book): Book = entity.copy(_id = None)
  }
}
