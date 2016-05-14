package controllers

import javax.inject.Inject

import models.{Book, User}
import play.api.Play.current
import play.api.data.Form
import play.api.data.Forms._
import play.api.i18n.Messages
import play.api.i18n.Messages.Implicits._
import play.api.libs.json.Json
import play.api.mvc.Action
import securesocial.core.SecureSocial
import services.infoproviders.BookInfoProvider
import services.mongo.BooksMongoService
import services.secsocial.MyEnvironment

import scala.concurrent.Future

/**
  * Created by P. Akhmedzianov on 13.05.2016.
  */

object SearchType extends Enumeration {
  type SearchType = Value
  val ID, ISBN, AUTHOR, YEAR, SUBJECT = Value
}

case class SearchData(searchRequest:String)
object SearchData{
  val searchForm = Form(
    mapping {
      "searchRequest" -> nonEmptyText
    }(SearchData.apply)(SearchData.unapply)
  )
}

class SearchController @Inject()(override implicit val env: MyEnvironment,
                                 booksMongoService: BooksMongoService,
                                 bookInfoProvider: BookInfoProvider)
  extends securesocial.core.SecureSocial {
  val searchTypeNames_ = Array("ID", "ISBN", "AUTHOR", "YEAR", "SUBJECT", "PUBLISHER", "TITLE")
  val searchLimit_ = 100

  def searchBook(searchType: String, value: String) = Action.async { implicit request =>
    if (searchTypeNames_.contains(searchType)) {
      var userName = "Guest"
      var userAvatarUrlOption: Option[String] = None
      val mayBeUserOptionFuture = SecureSocial.currentUser
      val searchResultFuture: Future[Traversable[Book]] = searchType match {
        case "ID" =>
          val intOption = safeStringToInt(value)
          if (intOption.isDefined) {
            booksMongoService.findByCriteria(Map("_id" -> intOption.get), searchLimit_)
          } else Future.successful(Traversable())
        case "ISBN" => booksMongoService.findByCriteria(Map("isbn" -> value), searchLimit_)
        case "AUTHOR" => booksMongoService.findByCriteria(Map("bookAuthor" -> value), searchLimit_)
        case "PUBLISHER" => booksMongoService.findByCriteria(Map("publisher" -> value), searchLimit_)
        case "YEAR" =>
          val intOption = safeStringToInt(value)
          if (intOption.isDefined) {
            booksMongoService.findByCriteria(Map("yearOfPublication" -> intOption.get), searchLimit_)
          } else Future.successful(Traversable())
        case "SUBJECT" => booksMongoService.findByCriteria(Map("subjects" -> value), searchLimit_)
        case "TITLE" => booksMongoService.findByCriteria(Map("$text" -> Json.obj("$search" -> value)), searchLimit_)
      }
      for {
        maybeUserOption <- mayBeUserOptionFuture
        booksTraversableSearchResult <- searchResultFuture
      } yield {
        if (maybeUserOption.isDefined) {
          val user = maybeUserOption.get.asInstanceOf[User]
          userName = user.main.fullName.get
          userAvatarUrlOption = user.main.avatarUrl
        }
        val bookList = booksTraversableSearchResult.toList

          Ok(views.html.search(userName,userAvatarUrlOption, bookList,
            SearchData.searchForm)(implicitly[Messages],
            implicitly[MyEnvironment]))
      }
    }
    else {
      Future.successful(BadRequest)
    }
  }

  def submitSearch = Action(parse.form(SearchData.searchForm)) { request =>
    val searchData = request.body
    Redirect(routes.SearchController.searchBook("TITLE", searchData.searchRequest))
  }

  def safeStringToInt(str: String): Option[Int] = try {
    Some(str.toInt)
  } catch {
    case e:NumberFormatException => None
  }
}
