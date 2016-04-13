package services.infoproviders

import javax.inject.Inject

import models.Book
import services.mongo.BooksMongoService

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by P. Akhmedzianov on 07.04.2016.
  */
class BookInfoProvider @Inject() (booksMongoService: BooksMongoService) {
  val MAIN_SLIDER_SIZE = 5

  var mainSliderBooks = Array(1,2,3,71,6661)
  val popularBooksIndexFlatList = Array(6661,66661,666661,1011,2551,10000)

  def getMainSliderBooks():Option[List[Book]]={
    getBookEntitiesByIdArray(mainSliderBooks)
  }

  def popularBooksForIndexFlatList():Option[List[Book]]={
    getBookEntitiesByIdArray(popularBooksIndexFlatList)
  }

  def getBookEntitiesByIdArray(arrayOfId: Array[Int]): Option[List[Book]] = {
    if (arrayOfId.length > 0) {
      val futureList = booksMongoService.getArrayBookByArrayId(arrayOfId.toList)
      val rateOptionFromMongo = Await.ready(futureList, Duration.Inf).value
      if(rateOptionFromMongo.isDefined && rateOptionFromMongo.get.isSuccess){
        Some(rateOptionFromMongo.get.get.flatMap(x=>x))
      }
      else None
    }
    else None
  }

}
