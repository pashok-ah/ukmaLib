package dataload

import javax.inject.Inject

import models.Book
import services.mongo.BooksMongoService
import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
  * Created by P. Akhmedzianov on 16.02.2016.
  */
class BooksToMongo @Inject()(booksMongoService: BooksMongoService)
  extends ImportToMongoObj[Book] {
  val filePath = "input/books_from_open_library.txt"
  val filterFilePath = "input/books_more_then_nine_rates.csv"

  val isbnsArray = Source.fromFile(filterFilePath).getLines.toList

  override val numFieldsToCheck = 9
  override val splitFieldsDelimiter = """\^"""

  var id = 0

  def processTheLine(line: String) = {
    getArray(line) match {
      case Array(isbn, bookTitle, bookAuthor, yearOfPublication, publisher, imageURLs, imageURLm, imageURLl, subjects) =>
        val subjectsArrayOption = processTheSubjects(subjects)
        subjectsArrayOption match {
          case Some(subjectsArray) => {
            if(isbnsArray.contains(isbn)){
            id += 1
            val book = new Book(Some(id), isbn, bookTitle, bookAuthor, yearOfPublication.toShort,
              publisher, imageURLs, imageURLm, imageURLl, subjectsArray, Array(), Array(), 0, None, None)
            bulkInsertListBuffer += book
            }
          }
          case _ =>
        }
    }
  }

  def insertListBuffer: Unit = {
    booksMongoService.bulkInsert(bulkInsertListBuffer)
    bulkInsertListBuffer.clear()
  }

  val subjectsToFilter = Array("Accessible book", "Protected DAISY",
    "In library", "etc", "OverDrive")

  def processTheSubjects(subjectsStr: String): Option[Array[String]] = {
    var listOfInputStrs = subjectsStr.split(splitArraysDelimiter).to[ListBuffer]
    listOfInputStrs = listOfInputStrs.--=(subjectsToFilter)
    if (listOfInputStrs.size > 0) Some(listOfInputStrs.toArray)
    else None
  }
}
