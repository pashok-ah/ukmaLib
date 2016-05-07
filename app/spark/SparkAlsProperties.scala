package spark

/**
  * Created by P. Akhmedzianov on 08.04.2016.
  */
case class AlsConfiguration(// default ALS parameters
                            rank_ : Int = 20,
                            numIterations_ : Int = 40,
                            lambda_ : Double = 1.0,
                            trainingShareInArraysOfRates_ : Double = 0.55,
                            validationShareAfterSubtractionTraining_ : Double = 0.5,
                            //lists for hyper-parameter grid search
                            ranksList_ : List[Int] = List(20),
                            lambdasList_ : List[Double] = List(0.1, 1.0),
                            numbersOfIterationsList_ : List[Int] = List(20),
                           //parameters for Random search
                            numberOfSearchSteps_ :Int = 10,
                            stepRadius_ : Double = 1.0)

object SparkAlsPropertiesLoader {
  val defaultPath = "conf/sparkAlsConfiguration.xml"

  def saveToDisk(path: String, alsProperties: AlsConfiguration): Unit = {
    scala.xml.XML.save(path, getXmlAlsConfiguration(alsProperties))
  }

  def loadFromDisk(path: String): AlsConfiguration = {
    try {
      val loadNode = xml.XML.loadFile(path)
      fromXML(loadNode)
    }
    catch {
      case e:NumberFormatException => {
        println("Caught NumberFormatException! Wrong Inputs! Using default values!")
        new AlsConfiguration()
      }
      case _:Throwable => {
        println("Caught Unknown Exception! Using default values!")
        new AlsConfiguration()
      }
    }
  }

  def fromXML(node: scala.xml.Node): AlsConfiguration =
    new AlsConfiguration {
      override val rank_ = (node \ "rank").text.toInt
      override val numIterations_ = (node \ "numIterations").text.toInt
      override val lambda_ = (node \ "lambda").text.toDouble
      override val trainingShareInArraysOfRates_ = (node \ "trainingShare").text.toDouble
      override val validationShareAfterSubtractionTraining_ = (node \ "validationShare").text.toDouble
      override val ranksList_ = (node \\ "rankVariant").iterator.map(_.text.toInt).toList
      override val lambdasList_ = (node \\ "lambdaVariant").iterator.map(_.text.toDouble).toList
      override val numbersOfIterationsList_ = (node \\ "numberOfIterationsVariant").iterator.map(_.text.toInt).toList
      override val numberOfSearchSteps_ = (node \ "numberOfSearchSteps").text.toInt
      override val stepRadius_ = (node \ "stepRadius").text.toDouble
    }


  def getXmlAlsConfiguration(alsProperties: AlsConfiguration) = {
    <alsConfiguration>
      <rank>{alsProperties.rank_}</rank>
      <numIterations>{alsProperties.numIterations_}</numIterations>
      <lambda>{alsProperties.lambda_}</lambda>
      <trainingShare>{alsProperties.trainingShareInArraysOfRates_}</trainingShare>
      <validationShare>{alsProperties.validationShareAfterSubtractionTraining_}</validationShare>
      <ranks>
        {alsProperties.ranksList_.map(rank => <rankVariant>{rank}</rankVariant>)}
      </ranks>
      <lambdas>
        {alsProperties.lambdasList_.map(lambda => <lambdaVariant>{lambda}</lambdaVariant>)}
      </lambdas>
      <numbersIterations>
        {alsProperties.numbersOfIterationsList_.map(numberOfIterations =>
        <numberOfIterationsVariant>{numberOfIterations}</numberOfIterationsVariant>)}
      </numbersIterations>
      <numberOfSearchSteps>{alsProperties.numberOfSearchSteps_}</numberOfSearchSteps>
      <stepRadius>{alsProperties.stepRadius_}</stepRadius>
    </alsConfiguration>
  }
}
