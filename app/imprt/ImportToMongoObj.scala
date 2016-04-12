package dataload

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by P. Akhmedzianov on 18.02.2016.
  */
trait ImportToMongoObj[T] {
  val filePath:String
  val encoding = "UTF8"

  val numberToBulkInsert = 5000

  val numFieldsToCheck = 3

  val splitFieldsDelimiter = "\";"
  val splitArraysDelimiter = ",";


  val bulkInsertListBuffer:ListBuffer[T] = new ListBuffer[T]
  def processTheLine(line:String)
  def insertListBuffer:Unit

  def importToMongo: Unit ={
    for(line <- Source.fromFile(filePath,encoding).getLines()){
      processTheLine(line)
      if (bulkInsertListBuffer.size >= numberToBulkInsert) {
        insertListBuffer
      }
    }
    //finally adding the rest of the users
    insertListBuffer
    bulkInsertListBuffer.clear()
  }

  def getArray(inputStr:String):Array[String] ={
    val arr:Array[String] = inputStr.split(splitFieldsDelimiter)
    for (i <- arr.indices){
      arr(i) = arr(i).replaceAll("\"","")
    }
    if(arr.size == numFieldsToCheck) arr else throw new Exception("Wrong parse! Different number of fields! " +
      "Read id: "+arr(0)+" Whole input string: "+inputStr)
  }

  val agePattern = "([0-9]+)".r
  def readAge(strAge: String): Option[Short] = {
    strAge match {
      case agePattern(age) => Some(age.toShort)
      case _ => None
    }
  }
}
