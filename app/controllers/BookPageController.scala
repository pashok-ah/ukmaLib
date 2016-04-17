package controllers

import javax.inject.Inject

import dataimport.AdditionalBooksInfoLoaderGoogleAPI
import models.{MyRating, User}
import play.api.mvc.Action
import play.api.routing.JavaScriptReverseRouter
import reactivemongo.bson.BSONObjectID
import securesocial.core.SecureSocial
import services.infoproviders.BookInfoProvider
import services.mongo._
import services.secsocial.MyEnvironment

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Created by P. Akhmedzianov on 05.04.2016.
  */
class BookPageController @Inject()(override implicit val env: MyEnvironment,
                                   booksMongoService: BooksMongoService,
                                   myRatingsMongoService: MyRatingMongoService,
                                   bookInfoProvider: BookInfoProvider)
  extends securesocial.core.SecureSocial {
  val additionalInfoLoader_ = new AdditionalBooksInfoLoaderGoogleAPI()
  val awaitDuration_ = 5 seconds


  def isRateInBorders(rate: Double): Boolean = rate > 0 && rate <= 10

  def isUserGetId(maybeUserOption: Option[BookPageController.this.env.U]) : Int = {
    if (maybeUserOption.isDefined && maybeUserOption.get.isInstanceOf[User])
      maybeUserOption.get.userIntId.get
    else
      -1
  }

  def getBook(bookId: Int) = Action.async { implicit request =>
    var userName = "Guest"
    var userAvatarUrlOption: Option[String] = None
    var rateResultOption: Option[MyRating] = None

    val bookOptionFuture = booksMongoService.getBookById(bookId)
    val mayBeUserOptionFuture = SecureSocial.currentUser

    for {
      bookOption <- bookOptionFuture
      maybeUserOption <- mayBeUserOptionFuture
      similarBooksList <- bookInfoProvider.getSimilarBooks(bookOption)
      youMayAlsoLikeBooksList <- bookInfoProvider.getYouMayAlsoLikeBooks(bookOption)
      rateOption <- myRatingsMongoService.getRateByUserAndBookIds(isUserGetId(maybeUserOption),
        bookId)
    } yield {
      if(maybeUserOption.isDefined){
        val user = maybeUserOption.get.asInstanceOf[User]
        userName = user.main.fullName.get
        userAvatarUrlOption = user.main.avatarUrl
      }
      if (rateOption.isDefined) rateResultOption = rateOption
      if (bookOption.isDefined) {
        var book = bookOption.get
        // if there is no desription trying to download from google api
        if (!book.description.isDefined) {
          book = book.copy(description = Some(handleBookDescription(book.isbn, book._id.get)))
        }
        Ok(views.html.book(book, similarBooksList , youMayAlsoLikeBooksList, rateOption, userName,
          userAvatarUrlOption))
      }
      else {
        NotFound(views.html.errors.error("Book not found", request.uri, userName, userAvatarUrlOption))
      }
    }
  }

  def saveTheRateAjaxCall(bookId: Int, rate: Double) = SecuredAction.async { implicit request =>
    if (isRateInBorders(rate)) {
      val createResultFuture = myRatingsMongoService.create(new MyRating(None,
        request.user.userIntId.get, bookId, rate))
      val httpResponse = createResultFuture.map {
        case Right(res) => Ok(res.stringify)
        case _ => Ok("Error")
      }
      httpResponse
    }
    else {
      Future.successful(Ok("Error"))
    }
  }

  def updateTheRateAjaxCall(myRatingId: String, rate: Double) = SecuredAction.async { implicit request =>
    val bsonMyRatingIdTry = BSONObjectID.parse(myRatingId)
    if (bsonMyRatingIdTry.isSuccess && isRateInBorders(rate)) {
      val updateFuture = myRatingsMongoService.updateExistingRating(bsonMyRatingIdTry.get, rate)
      val httpResponse = updateFuture.map {
        case updateWriteResult if updateWriteResult.ok  => Ok("Success")
        case _ => Ok("Error")
      }
      httpResponse
    }
    else {
      Future.successful(Ok("Error"))
    }
  }

  def deleteTheRateAjaxCall(myRatingId: String) = SecuredAction.async { implicit request =>
    val bsonMyRatingIdTry = BSONObjectID.parse(myRatingId)
    if (bsonMyRatingIdTry.isSuccess) {
      val deleteFuture = myRatingsMongoService.delete(bsonMyRatingIdTry.get)
      val httpResponse = deleteFuture.map {
        case Right(res) => Ok("Success")
        case _ => Ok("Error")
      }
      httpResponse
    }
    else {
      Future.successful(Ok("Error"))
    }
  }

  def javascriptRoutes = Action.async { implicit request =>
    Future.successful(Ok(
      JavaScriptReverseRouter("jsRoutes")(
        routes.javascript.BookPageController.saveTheRateAjaxCall,
        routes.javascript.BookPageController.updateTheRateAjaxCall,
        routes.javascript.BookPageController.deleteTheRateAjaxCall
      )
    ).as("text/javascript"))
  }

  def handleBookDescription(isbn: String, bookId: Int): String = {
    var resultDescription = "No description."
    val description = additionalInfoLoader_.getInfoByIsbn(isbn)
    if (description != "None") resultDescription = description
    // saving to database the result
    booksMongoService.updateDescriptionById(bookId, resultDescription)
    resultDescription
  }
}
