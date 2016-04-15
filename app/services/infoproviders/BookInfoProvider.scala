package services.infoproviders

import javax.inject.Inject

import models.Book
import services.mongo.BooksMongoService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by P. Akhmedzianov on 07.04.2016.
  */
class BookInfoProvider @Inject()(booksMongoService: BooksMongoService) {
  val MAIN_SLIDER_SIZE = 5

  var mainSliderBooks = Array(1, 2, 3, 71, 6661)
  val popularBooksIndexFlatList = Array(6661, 66661, 666661, 1011, 2551, 10000)

  def getMainSliderBooks(): Future[List[Book]] = {
    getBookEntitiesByIdArray(mainSliderBooks)
  }

  def popularBooksForIndexFlatList(): Future[List[Book]] = {
    getBookEntitiesByIdArray(popularBooksIndexFlatList)
  }

  def getBookEntitiesByIdArray(arrayOfId: Array[Int]): Future[List[Book]] = {
    if (arrayOfId.length > 0) {
      booksMongoService.getArrayBookByArrayId(arrayOfId.toList).map(list =>
        list.flatMap(x => x))
    }
    else Future.successful(List())
  }

  def getSimilarBooks(bookOption: Option[Book]): Future[List[Book]] = {
    if(bookOption.isDefined){
      getBookEntitiesByIdArray(bookOption.get.similarBooks)
    }
    else{
      Future.successful(List())
    }
  }

  def getYouMayAlsoLikeBooks(bookOption: Option[Book]): Future[List[Book]] = {
    if(bookOption.isDefined){
      getBookEntitiesByIdArray(bookOption.get.youMayAlsoLikeBooks)
    }
    else{
      Future.successful(List())
    }
  }

}
