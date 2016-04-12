package services.secsocial

import javax.inject.Inject

import models.JsonFormats.mailTokenFormat
import models.{JsonFormats, User}
import org.joda.time.DateTime
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json
import play.modules.reactivemongo.ReactiveMongoApi
import play.modules.reactivemongo.json._
import play.modules.reactivemongo.json.collection.JSONCollection
import securesocial.core.providers.MailToken
import securesocial.core.services.{SaveMode, UserService}
import securesocial.core.{BasicProfile, PasswordInfo}
import services.mongo.UsersMongoService
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
/**
  * Created by P. Akhmedzianov on 05.02.2016.
  */
class MyMongoUserService extends UserService[User] {
  val usersMongoService = new UsersMongoService
  lazy val reactiveMongoApi = current.injector.instanceOf[ReactiveMongoApi]
  def tokens(): JSONCollection = reactiveMongoApi.db.collection[JSONCollection]("tokens")

  def find(providerId: String, userId: String): Future[Option[BasicProfile]] = {
    usersMongoService.findByCriteria(Map("providerid" -> providerId, "securesocialid" -> userId), 1).map {
      case t if t.nonEmpty => Some(t.head.main)
      case _ => None
    }
  }

  def findByEmailAndProvider(email: String, providerId: String): Future[Option[BasicProfile]] = {
    usersMongoService.findByCriteria(Map("providerid" -> providerId, "email" -> email), 1).map {
      case t if t.nonEmpty => Some(t.head.main)
      case _ => None
    }
  }

  def save(basicProfile: BasicProfile, mode: SaveMode): Future[User] = {
    mode match {
      case SaveMode.SignUp =>
        Future(createNewUser(basicProfile))
      case SaveMode.LoggedIn =>
        usersMongoService.findByCriteria(Map("providerid" -> basicProfile.providerId,
          "email" -> basicProfile.email.get), 1).map {
          case t if t.nonEmpty => t.head
          case _ => createNewUser(basicProfile)
        }
      case SaveMode.PasswordChange =>
        usersMongoService.findByCriteria(Map("providerid" -> basicProfile.providerId,
          "email" -> basicProfile.email.get), 1).map {
          case t if t.nonEmpty => Await.ready(updateProfile(t.head, basicProfile), Duration.Inf).value.get.get
          case _ => createNewUser(basicProfile)
        }
    }
  }

  private def createNewUser(basicProfile: BasicProfile): User = {
    val newUser = new User(basicProfile, None, None, None, List())
    val futureRes = usersMongoService.create(newUser)
    Await.ready(futureRes, Duration.Inf).value.get.get match {
      case Left(mes) => {
        throw new Exception("Creation of new user failed! "+mes)
        null
      }
      case Right(id) => newUser.copy(userIntId = Some(id))
    }
  }

  private def updateProfile(user: User, newProfile: BasicProfile): Future[User] = {
    val updatedUser = user.copy(main = newProfile)
    usersMongoService.update(user.userIntId.get, updatedUser) map {
      case Left(msg) => null
      case Right(id) => updatedUser
    }
  }

  override def updatePasswordInfo(user: User, newInfo: PasswordInfo): Future[Option[BasicProfile]] = {
    val updatedUser = user.copy(main = user.main.copy(passwordInfo = Some(newInfo)))
    usersMongoService.update(user.userIntId.get, updatedUser) map {
      case Left(msg) => None
      case Right(id) => Some(updatedUser.main)
    }
  }

  override def passwordInfoFor(user: User): Future[Option[PasswordInfo]] = {
    Future.successful(user.main.passwordInfo)
  }

  // token functions implementation for userpass register
  def saveToken(token: MailToken): Future[MailToken] = Future.successful {
    tokens().insert(token)
    token
  }

  def findToken(uuid: String): Future[Option[MailToken]] = {
    val futureToken = tokens.find(Json.obj("uuid" -> uuid)).one[MailToken]
    futureToken
  }

  def deleteToken(uuid: String): Future[Option[MailToken]] = {
    val selector = Json.obj("uuid" -> uuid)
    val res = tokens.find(selector).one[MailToken]
    tokens.remove(selector)
    res
  }

  def deleteExpiredTokens() {
    val selector = Json.obj("expirationTime" -> Json.obj("$lt" -> Json.obj("$date" -> DateTime.now().getMillis)))
    tokens.remove(selector)
  }

  //this function is not supported
  def link(current: User, to: BasicProfile): Future[User] = ???
}
