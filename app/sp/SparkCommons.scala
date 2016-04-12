package sp

/**
  * Created by P. Akhmedzianov on 17.02.2016.
  */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import play.api.i18n.Messages.Implicits._
import play.api.Play.current
import play.api.i18n.Messages
object SparkCommons {
  //build the SparkConf  object at once
   lazy val conf = {
    new SparkConf(false)
      .setMaster("local[*]")
      .setAppName(Messages("appName"))
      .set("spark.akka.logLifecycleEvents", "true")
  }

  lazy val sc = new SparkContext(conf)
  lazy val sqlContext = new SQLContext(sc)
}
