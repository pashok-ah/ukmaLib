package dataload

import javax.inject.Inject

import models.User
import securesocial.core.{AuthenticationMethod, BasicProfile}
import services.mongo.UsersMongoService

/**
  * Created by P. Akhmedzianov on 16.02.2016.
  */
class UsersToMongo @Inject()(usersMongoService: UsersMongoService)
  extends ImportToMongoObj[User] {
  val filePath = "input/BX-Users.csv"

  def processTheLine(line: String) = {
    getArray(line) match {
      case Array(userIntId, location, age) => {
        //generating 'email' for user from dataset
        val email = location.split(",")(0).replaceAll(" ", "_") + userIntId + "@gmail.com"
        bulkInsertListBuffer += User(new BasicProfile("userpass", email, None, None, None, Some(email),
          None, AuthenticationMethod("userPassword"), None, None, None), Some(userIntId.toInt),
          Some(location), readAge(age), List(), Array())
      }
    }
  }

  def insertListBuffer(): Unit = {
    usersMongoService.bulkInsert(bulkInsertListBuffer)
    bulkInsertListBuffer.clear()
  }
}
