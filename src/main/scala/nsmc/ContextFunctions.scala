package nsmc

import nsmc.mongo.{MongoConnectorConf, CollectionConfig}

import nsmc.rdd.MongoRDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class ContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def mongoCollection[T](databaseName: String, collectionName: String, indexedKeys:Seq[(String,Any)] = Seq())
                       (implicit ct: ClassTag[T]) = {
    val collectionConfig = new CollectionConfig(MongoConnectorConf(sc.getConf), databaseName, collectionName, indexedKeys)
    new MongoRDD[T](sc, collectionConfig)
  }

}