package controllers

import javax.inject.Inject

import play.api.Logger
import play.api.mvc.{Action, AnyContent, RequestHeader}
import securesocial.controllers.BaseLoginPage
import securesocial.core.{IdentityProvider, RuntimeEnvironment}
import securesocial.core.services.RoutesService

class CustomLoginController @Inject() (implicit override val env: RuntimeEnvironment) extends BaseLoginPage {
  override def login: Action[AnyContent] = {
    Logger.debug("using CustomLoginController")
    super.login
  }
}

class CustomRoutesService extends RoutesService.Default {
  override def loginPageUrl(implicit req: RequestHeader): String =
    controllers.routes.CustomLoginController.login().absoluteURL(IdentityProvider.sslEnabled)
}