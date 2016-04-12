package models

import models.Identity
import play.api.libs.json._
import securesocial.core._
import play.modules.reactivemongo.json._
import scala.util.{Failure, Success}

/**
  * Created by P. Akhmedzianov on 04.02.2016.
  */
case class User(main: BasicProfile,
                userIntId: Option[Int],
                location: Option[String],
                age: Option[Short],
                roles: List[String] = List(),
                personalRecommendations: Array[Int] = Array())

object User {
  implicit def user2JsObject(value: User) = UserWrites.writes(value)


  implicit object UserWrites extends OWrites[User] {
    def writes(user: User): JsObject = Json.obj(
        //Basic profile fields
        "securesocialid" -> user.main.userId,
        "providerid" -> user.main.providerId,
        "firstname" -> user.main.firstName,
        "lastname" -> user.main.lastName,
        "email" -> user.main.email,
        "avatarurl" -> user.main.avatarUrl,
        "authmethod" -> user.main.authMethod.method,
        "oauth1" -> (user.main.oAuth1Info map { oAuth1Info => Json.obj(
          "token" -> oAuth1Info.token,
          "secret" -> oAuth1Info.secret
        )
        }),
        "oauth2" -> (user.main.oAuth2Info map { oAuth2Info => Json.obj(
          "accessToken" -> oAuth2Info.accessToken,
          "tokenType" -> oAuth2Info.tokenType,
          "expiresIn" -> oAuth2Info.expiresIn,
          "refreshToken" -> oAuth2Info.refreshToken
        )
        }),
        "password" -> (user.main.passwordInfo map { passwordInfo => Json.obj(
          "hasher" -> passwordInfo.hasher,
          "password" -> passwordInfo.password,
          "salt" -> passwordInfo.salt
        )
        }),
        //Additional fields
        "_id" -> user.userIntId,
        "location" -> user.location,
        "age" -> user.age,
        "roles" -> user.roles,
        "personalRecommendations" -> user.personalRecommendations
      )

  }

  implicit object UserReads extends Reads[User] {
    def reads(json: JsValue): JsResult[User] = json match {
      case obj: JsObject => try {
        val userid = (json \ "securesocialid").as[String]
        val providerid = (json \ "providerid").as[String]
        val firstname = (json \ "firstname").asOpt[String]
        val lastname = (json \ "lastname").asOpt[String]
        /*val fullname = (json \ "fullname").asOpt[String]*/
        val fullname = firstname.getOrElse(null) +" "+ lastname.getOrElse(null)
        val email = (json \ "email").as[String]
        val avatarurl = (json \ "avatarurl").asOpt[String]
        val authmethod = (json \ "authmethod").as[String]
        //additional fields
        val userIntId = (json \ "_id").asOpt[Int]
        val location = (json \ "location").asOpt[String]
        val age = (json \ "age").asOpt[Short]
        val roles = (json \ "roles").as[List[String]]
        val personalRecommendations = (json \ "personalRecommendations").as[Array[Int]]
        // oauth2 reading
        val oauth2JsObj = (json \ "oauth2").asOpt[JsObject]
        val oauth2: Option[OAuth2Info] = oauth2JsObj match {
          case Some(obj: JsObject) =>
            val accessToken = (obj \ "accessToken").as[String]
            val tokenType = (obj \ "tokenType").asOpt[String]
            val expiresIn = (obj \ "expiresIn").asOpt[Int]
            val refreshToken = (obj \ "refreshToken").asOpt[String]
            Some(new OAuth2Info(accessToken, tokenType, expiresIn, refreshToken))
          case _ => None
        }
        // password reading
        val passwordJsObj = (json \ "password").asOpt[JsObject]
        val pwdInfo: Option[PasswordInfo] = passwordJsObj match {
          case Some(obj: JsObject) =>
            val hash = (obj \ "hasher").as[String]
            val password = (obj \ "password").as[String]
            val salt = (obj \ "salt").asOpt[String]
            Some(new PasswordInfo(hash, password))
          case _ => None
        }
        val authMethod: AuthenticationMethod = new AuthenticationMethod(authmethod)

        val user: User = new User(new BasicProfile(providerid, userid, firstname, lastname, Some(fullname), Some(email),
          avatarurl, authMethod, None, oauth2, pwdInfo), userIntId, location, age, roles, personalRecommendations)
        JsSuccess(user)
      } catch {
        case cause: Throwable => JsError(cause.getMessage)
      }
      case _ => JsError("expected.jsobject")
    }
  }

  implicit object UserIdentity extends Identity[User, Int] {
    val name = "_id"
    def of(entity: User): Option[Int] = entity.userIntId
    def set(entity: User, id: Int): User = entity.copy(userIntId = Some(id))
    def clear(entity: User): User = entity.copy(userIntId = None)
    def next: Int = {
      val res = CounterHandler.getNextSequenceUser("userid") match {
        case Success(t) => Some(Right(t).b.get.seq.toInt)
        case Failure(e) => null
      }
      res.get
    }
  }
}

