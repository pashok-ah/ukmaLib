package services.secsocial

import play.api.data.Form
import play.api.i18n.Lang
import play.api.mvc.RequestHeader
import play.twirl.api.Html
import securesocial.controllers.{ChangeInfo, RegistrationInfo, ViewTemplates}
import securesocial.core.RuntimeEnvironment
import play.api.Play.current
import play.api.i18n.Messages.Implicits._

/**
  * Created by P. Akhmedzianov on 19.03.2016.
  */
class MyViewTemplates(env: RuntimeEnvironment) extends ViewTemplates{
  implicit val implicitEnv = env

  override def getLoginPage(form: Form[(String, String)], msg: Option[String])(implicit request: RequestHeader, lang: Lang): Html = {
    views.html.secsocial.login(form, msg)
  }

  override def getPasswordChangePage(form: Form[ChangeInfo])(implicit request: RequestHeader, lang: Lang): Html = {
    views.html.secsocial.passwordChange(form)
  }

  override def getNotAuthorizedPage(implicit request: RequestHeader, lang: Lang): Html = {
    views.html.secsocial.notAuthorized()
  }

  override def getStartSignUpPage(form: Form[String])(implicit request: RequestHeader, lang: Lang): Html = {
    views.html.secsocial.registration.startSignUp(form)
  }

  override def getSignUpPage(form: Form[RegistrationInfo], token: String)(implicit request: RequestHeader, lang: Lang): Html = {
    views.html.secsocial.registration.signUp(form, token)
  }

  override def getResetPasswordPage(form: Form[(String, String)], token: String)(implicit request: RequestHeader, lang: Lang): Html = {
    views.html.secsocial.registration.resetPasswordPage(form, token)
  }

  override def getStartResetPasswordPage(form: Form[String])(implicit request: RequestHeader, lang: Lang): Html = {
    views.html.secsocial.registration.startResetPassword(form)
  }
}
