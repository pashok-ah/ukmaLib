/**
  * Copyright 2012 Jorge Aliss (jaliss at gmail dot com) - twitter: @jaliss
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
package controllers

import javax.inject.Inject

import _root_.services.infoproviders.BookInfoProvider
import _root_.services.secsocial.MyEnvironment
import models.User
import play.api.Play.current
import play.api.i18n.Messages
import play.api.i18n.Messages.Implicits._
import play.api.mvc.{Action, RequestHeader}
import securesocial.core._

import scala.concurrent.Future


class Application @Inject()(override implicit val env: MyEnvironment,
                            bookInfoProvider : BookInfoProvider)
  extends securesocial.core.SecureSocial {

  def index = Action.async { implicit request =>
    var userName = "Guest"
    var userAvatarUrlOption : Option[String]= None
    SecureSocial.currentUser.map { maybeUser =>
      if(maybeUser.isDefined && maybeUser.get.isInstanceOf[User]) {
        val user = maybeUser.get.asInstanceOf[User]
        userName = user.main.fullName.get
        userAvatarUrlOption = user.main.avatarUrl
      }
      Ok(views.html.index(userName, userAvatarUrlOption,bookInfoProvider.getMainSliderBooks(),
        bookInfoProvider.popularBooksForIndexFlatList)(implicitly[Messages], implicitly[MyEnvironment]))
    }
  }

  def profileInfo = SecuredAction.async { implicit request =>
    Future(Ok(views.html.profile(request.user.main)))
  }

  def recommendations = SecuredAction.async { implicit request =>
    Future(Ok(views.html.recommendations(request.user.main.fullName.get, request.user.main.avatarUrl,
      bookInfoProvider.getBookEntitiesByIdArray(request.user.personalRecommendations))(implicitly[Messages],
      implicitly[MyEnvironment])))
  }

  // a sample action using an authorization implementation
  def onlyTwitter = SecuredAction(WithProvider("facebook")) { implicit request =>
    Ok("You can see this because you logged in using Twitter")
  }

  def testFromMongoToRddImport = Action {
    Ok("Imported!!!")
  }
  /**
    * Sample use of SecureSocial.currentUser. Access the /current-user to test it
    */
  def currentUser = Action.async { implicit request =>
    SecureSocial.currentUser.map { maybeUser =>
      val userId = maybeUser.map(_.main.userId).getOrElse("unknown")
      Ok(s"Your id is $userId")
    }
  }
}

// An Authorization implementation that only authorizes uses that logged in using twitter
case class WithProvider(provider: String) extends Authorization[User] {
  def isAuthorized(user: User, request: RequestHeader) = {
    user.main.providerId == provider
  }
}
