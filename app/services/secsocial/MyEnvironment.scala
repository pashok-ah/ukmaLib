package services.secsocial

import javax.inject.Inject

import controllers.CustomRoutesService
import models.User
import securesocial.core.RuntimeEnvironment
import securesocial.core.providers.{FacebookProvider, UsernamePasswordProvider}
import services.mongo.UsersMongoService

import scala.collection.immutable.ListMap

class MyEnvironment
  extends RuntimeEnvironment.Default {
  override type U = User
  override implicit val executionContext = play.api.libs.concurrent.Execution.defaultContext
  override lazy val routes = new CustomRoutesService()
  override lazy val userService: MyMongoUserService = new MyMongoUserService()
  override lazy val eventListeners = List(new MyEventListener())
  override lazy val viewTemplates = new MyViewTemplates(this)
  override lazy val providers = ListMap(
    include(new FacebookProvider(routes, cacheService, oauth2ClientFor(FacebookProvider.Facebook))),
    include(new UsernamePasswordProvider[User](userService, avatarService, viewTemplates, passwordHashers))
  )
}

/*
class MyBasicEnvironment @Inject() (val env: MyEnvironment[U]) extends RuntimeEnvironment.Default[U] {
  override lazy val userService: InMemoryUserService = env.userService
}*/
