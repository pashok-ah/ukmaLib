import javax.inject._

import play.api._
import play.api.http.DefaultHttpErrorHandler
import play.api.mvc.Results._
import play.api.mvc._
import play.api.routing.Router

import scala.concurrent._

/**
  * Created by P. Akhmedzianov on 15.04.2016.
  */
class ErrorHandler @Inject() (
                               env: Environment,
                               config: Configuration,
                               sourceMapper: OptionalSourceMapper,
                               router: Provider[Router]
                             ) extends DefaultHttpErrorHandler(env, config, sourceMapper, router) {

  override def onProdServerError(request: RequestHeader, exception: UsefulException) = {
    Future.successful(InternalServerError(views.html.errors.error("A server error occurred: " +
      exception.getMessage, "", "Guest", None)))
  }

  override def onForbidden(request: RequestHeader, message: String) = {
    Future.successful(Forbidden(views.html.errors.error("You're not allowed to access this resource.",
      request.path, "Guest", None)))
  }

  override def onNotFound(request: RequestHeader, message: String): Future[Result] = {
    Future.successful(NotFound(views.html.errors.error("Page not found",
      request.path, "Guest", None)))
  }
}
