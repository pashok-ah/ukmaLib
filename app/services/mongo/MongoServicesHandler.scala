package services.mongo

/**
  * Created by P. Akhmedzianov on 10.04.2016.
  */
object MongoServicesHandler {
  lazy val booksMongoService = new BooksMongoService()
  lazy val usersMongoService = new UsersMongoService()
}
