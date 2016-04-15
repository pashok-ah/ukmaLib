package controllers

import javax.inject.Inject

import dataimport.AdditionalBooksInfoLoaderGoogleAPI
import models.{MyRating, User}
import play.api.Play.current
import play.api.i18n.Messages
import play.api.i18n.Messages.Implicits._
import play.api.mvc.Action
import play.api.routing.JavaScriptReverseRouter
import reactivemongo.bson.BSONObjectID
import securesocial.core.SecureSocial
import services.infoproviders.BookInfoProvider
import services.mongo._
import services.secsocial.MyEnvironment

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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

  def isUser(maybeUser: Option[BookPageController.this.env.U]) = {
    maybeUser.isDefined && maybeUser.get.isInstanceOf[User]
  }

  def getBook(bookId: Int) = Action.async { implicit request =>
    var userName = "Guest"
    var userAvatarUrlOption: Option[String] = None
    var rateOption: Option[MyRating] = None

    val bookFuture = booksMongoService.getBookById(bookId)
    val bookOption = Await.ready(bookFuture, awaitDuration_).value.get

    SecureSocial.currentUser.map { maybeUser =>
      if (isUser(maybeUser)) {
        val user = maybeUser.get.asInstanceOf[User]
        userName = user.main.fullName.get
        userAvatarUrlOption = user.main.avatarUrl
        // finding rate for this user in database
        if (bookOption.get.isDefined) {
          val rateFuture = myRatingsMongoService.getRateByUserAndBookIds(user.userIntId.get, bookId)
          val rateOptionFromMongo = Await.ready(rateFuture, awaitDuration_).value.get
          rateOption = rateOptionFromMongo.getOrElse(None)
        }
      }

      if (bookOption.get.isDefined) {
        var book = bookOption.get.get
        // if there is no desription trying to download from google api
        if (!book.description.isDefined) {
          book = book.copy(description = Some(handleBookDescription(book.isbn, book._id.get)))
        }
        Ok(views.html.book(book, bookInfoProvider.getBookEntitiesByIdArray(book.similarBooks),
          bookInfoProvider.getBookEntitiesByIdArray(book.youMayAlsoLikeBooks), rateOption, userName,
          userAvatarUrlOption)(implicitly[Messages], implicitly[MyEnvironment]))
      }
      else {
        NotFound
      }
    }
  }

  def saveTheRateAjaxCall(bookId: Int, rate: Double) = SecuredAction { implicit request =>
    if (isRateInBorders(rate)) {
      val createResultFuture = myRatingsMongoService.create(new MyRating(None,
        request.user.userIntId.get, bookId, rate))
      val resultMessage = awaitAndReturnBookId(createResultFuture)
      Ok(resultMessage)
    }
    else {
      Ok("Error")
    }
  }

  def updateTheRateAjaxCall(myRatingId: String, rate: Double) = SecuredAction { implicit request =>
    val bsonMyRatingIdTry = BSONObjectID.parse(myRatingId)
    if (bsonMyRatingIdTry.isSuccess && isRateInBorders(rate)) {
      val updateFuture = myRatingsMongoService.updateExistingRating(bsonMyRatingIdTry.get, rate)
      val awaitResult = Await.ready(updateFuture, awaitDuration_).value.get
      if(awaitResult.isSuccess && awaitResult.get.ok)
        Ok("Success")
      else
        Ok("Error")
    }
    else {
      Ok("Error")
    }
  }

  def deleteTheRateAjaxCall(myRatingId: String) = SecuredAction { implicit request =>
    val bsonMyRatingIdTry = BSONObjectID.parse(myRatingId)
    if (bsonMyRatingIdTry.isSuccess) {
      myRatingsMongoService.delete(bsonMyRatingIdTry.get)
      Ok("Success")
    }
    else {
      Ok("Error")
    }
  }

  def awaitAndReturnBookId(future: Future[Either[String, BSONObjectID]]): String = {
    var resultMessage = "Error"
    val awaitResult = Await.ready(future, awaitDuration_).value.get
    if (awaitResult.isSuccess) {
      awaitResult.get match {
        case Right(res) => resultMessage = res.stringify
        case _ =>
      }
    }
    resultMessage
  }

  def javascriptRoutes = Action { implicit request =>
    Ok(
      JavaScriptReverseRouter("jsRoutes", Some("myAjaxFunction"))(
        routes.javascript.BookPageController.saveTheRateAjaxCall,
        routes.javascript.BookPageController.updateTheRateAjaxCall,
        routes.javascript.BookPageController.deleteTheRateAjaxCall
      )
    ).as("text/javascript")
  }

  def handleBookDescription(isbn: String, bookId: Int): String = {
    //if no description in database, trying to load from google books api
    var resultDescription = "No description."
    val description = additionalInfoLoader_.getInfoByIsbn(isbn)
    if (description != "None") resultDescription = description
    // saving to database the result
    booksMongoService.updateDescriptionById(bookId, resultDescription)
    resultDescription
  }
}
