package nsmc

import nsmc.mongo.{MongoConnectorConf, CollectionConfig, MongoConnector}

import nsmc.rdd.MongoRDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class ContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def mongoCollection[T](databaseName: String, collectionName: String, indexedKays:Seq[String] = Seq())
                       (implicit ct: ClassTag[T]) = {
    val collectionConfig = new CollectionConfig(MongoConnectorConf(sc.getConf), databaseName, collectionName, indexedKays)
    new MongoRDD[T](sc, collectionConfig)
  }
}