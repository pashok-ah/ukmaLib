package spark

/**
  * Created by P. Akhmedzianov on 17.02.2016.
  */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import play.api.Play.current
import play.api.i18n.Messages
import play.api.i18n.Messages.Implicits._

object SparkCommons {
  //build the SparkConf  object at once
   lazy val conf = {
    new SparkConf(false)
      .setMaster("local[*]")
      .setAppName(Messages("appName"))
      .set("spark.akka.logLifecycleEvents", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","24m")
      .set("spark.kryo.registrator", "spark.MyKryoRegistrator")
/*      .set("spark.kryo.registrationRequired", "true")*/
  }

  lazy val sc = new SparkContext(conf)
  lazy val sqlContext = new SQLContext(sc)
}


