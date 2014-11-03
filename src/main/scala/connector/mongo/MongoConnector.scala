package connector.mongo

import com.mongodb.casbah.Imports._
import org.apache.spark.SparkConf

class MongoConnector(conf: MongoConnectorConf) extends Serializable {

  def getCollection(databaseName: String, collectionName: String): Iterator[MongoDBObject] = {
    val mongoClient = MongoClient(conf.host, conf.port)
    // TODO: deal with credentials
    val db = mongoClient.getDB(databaseName)
    val col = db(collectionName)
    col.iterator.map(e => e)
  }

}

object MongoConnector {
  def apply(conf: SparkConf): MongoConnector = {
    new MongoConnector(MongoConnectorConf(conf))
  }
}
