package dataload

import java.util.{Map, LinkedHashMap}
import javax.inject.Inject

import models.MyRating
import reactivemongo.bson.BSONObjectID
import services.secsocial.MyEnvironment
import scala.concurrent.Await
import scala.concurrent.duration._
import services.mongo.{MyRatingMongoService, BooksMongoService}
/**
  * Created by P. Akhmedzianov on 16.02.2016.
  */
class RatesToMongo @Inject()(myRatingsMongoService: MyRatingMongoService,
                             booksMongoService: BooksMongoService)
  extends ImportToMongoObj[MyRating]{
  val filePath = "input/BX-Book-Ratings.csv"
  override val encoding = "ISO8859_1"

  //small cache for storing already found ids
  val cacheIsbnToId = makeCache[String,Int](1000)
  var putFromCache = 0

  def processTheLine(line: String) = {
    getArray(line) match {
      case Array(userId, isbn, rate) => {
        Option(cacheIsbnToId.get(isbn)) match {
          //case, there is such isbn in cache
          case Some(uid) =>
            bulkInsertListBuffer +=new MyRating(Some(BSONObjectID.generate),userId.toInt, uid, rate.toDouble)
            putFromCache+=1
            cacheIsbnToId.put(isbn, uid)
          //case, when no such isbn in cache
          case None =>
            val futureOptBookId = booksMongoService.getBookIdByISBN(isbn)
            val result = Await.result(futureOptBookId, 3.seconds)
            result match {
              case Some(bookId) =>
                cacheIsbnToId.put(isbn, bookId)
                bulkInsertListBuffer +=new MyRating(Some(BSONObjectID.generate),userId.toInt, bookId, rate.toDouble)
              case None =>
            }
        }
      }
    }
  }

   def insertListBuffer():Unit={
     myRatingsMongoService.bulkInsert(bulkInsertListBuffer)
    System.out.println("Put from cache:"+putFromCache+" Bulk size:"+bulkInsertListBuffer.size)
    putFromCache=0
    bulkInsertListBuffer.clear ()
  }

  def makeCache[K,V](capacity: Int): Map[K, V] = {
    new LinkedHashMap[K, V](capacity, 0.7F, true) {
      private val cacheCapacity = capacity

      override def removeEldestEntry(entry: Map.Entry[K, V]): Boolean = {
        this.size() > this.cacheCapacity
      }
    }
  }
}

