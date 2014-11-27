package nsmc

import nsmc.mongo.MongoConnector

import nsmc.rdd.MongoRDD
import org.apache.spark.SparkContext

import scala.reflect.ClassTag

class ContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def mongoCollection[T](databaseName: String, collectionName: String)
                       (implicit connector: MongoConnector = MongoConnector(sc.getConf),
                        ct: ClassTag[T]) =
    new MongoRDD[T](sc, connector, databaseName, collectionName)
}