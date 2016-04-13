package spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by P. Akhmedzianov on 13.04.2016.
  */
class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SubjectsSparkRecommender])
    kryo.register(classOf[MlLibAlsSparkRatingsFromMongoHandler])
    kryo.register(classOf[BookGlobalRatingsUpdater])
    kryo.register(classOf[ContentBasedSparkRatingsRecommender])
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"))
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))
    kryo.register(Class.forName("org.apache.spark.ml.recommendation.ALS$RatingBlock"))
    kryo.register(Class.forName("scala.math.Ordering$$anon$9"))
    kryo.register(Class.forName("scala.math.Ordering$$anonfun$by$1"))
    kryo.register(Class.forName("scala.math.Ordering$Double$"))
    kryo.register(Class.forName("org.apache.spark.mllib.recommendation.MatrixFactorizationModel$$anonfun$org$apache$spark$mllib$recommendation$MatrixFactorizationModel$$recommendForAll$1"))
    kryo.register(classOf[Array[scala.Tuple2[_,_]]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[Array[Float]]])
    kryo.register(classOf[Array[Double]])
  }
}
