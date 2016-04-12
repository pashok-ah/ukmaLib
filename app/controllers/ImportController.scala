package controllers

import javax.inject.Inject

import dataload.{RatesToMongo, BooksToMongo, UsersToMongo}
import services.secsocial.MyEnvironment

import scala.concurrent.Future

/**
  * Created by P. Akhmedzianov on 10.04.2016.
  */
class ImportController @Inject() (override implicit val env: MyEnvironment,
                                  usersToMongo: UsersToMongo,
                                  booksToMongo: BooksToMongo,
                                  ratesToMongo: RatesToMongo)
  extends securesocial.core.SecureSocial{

  def importUsers = SecuredAction.async { implicit request =>
    usersToMongo.importToMongo
    Future(Ok("Successfully imported users!"))
  }

  def importBooks = SecuredAction.async { implicit request =>
    booksToMongo.importToMongo
    Future(Ok("Successfully imported books!"))
  }

  def importRatings = SecuredAction.async { implicit request =>
    ratesToMongo.importToMongo
    Future(Ok("Successfully imported rates!"))
  }
}
