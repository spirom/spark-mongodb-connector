package nsmc.mongo

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import org.apache.spark.SparkConf

class MongoConnector(conf: MongoConnectorConf) extends Serializable {

  def getCollection(databaseName: String, collectionName: String): Iterator[MongoDBObject] = {
    val server = new ServerAddress(conf.host, conf.port)
    val mongoClient = if (conf.user.isDefined && conf.password.isDefined) {
      val credentials = MongoCredential.createMongoCRCredential(conf.user.get, databaseName, conf.password.get.toCharArray)
      MongoClient(server, List(credentials))
    } else {
      MongoClient(server)
    }
    val db = mongoClient.getDB(databaseName)
    val col = db(collectionName)
    col.iterator.map(e => e)
  }
}

object MongoConnector {
  def apply(conf: SparkConf): MongoConnector = {
    new MongoConnector(MongoConnectorConf(conf))
  }

  // fetch data from the given interval
  def getCollection(databaseName: String, collectionName: String, interval: MongoInterval): Iterator[DBObject] = {

    val server = new ServerAddress(interval.destination.host, interval.destination.port)
    val conf = interval.destination.conf
    val mongoClient = if (conf.user.isDefined && conf.password.isDefined) {
      val credentials = MongoCredential.createMongoCRCredential(conf.user.get, databaseName, conf.password.get.toCharArray)
      MongoClient(server, List(credentials))
    } else {
      MongoClient(server)
    }

    val db = mongoClient.getDB(databaseName)
    val col = db(collectionName)

    val cursor = col.find()
    val withMin = if (interval.min == null || interval.min.values.size == 0) cursor else cursor.addSpecial("$min", interval.min)
    val withMax = if (interval.max == null || interval.max.values.size == 0) withMin else cursor.addSpecial("$max", interval.max)
    withMax.toIterator
  }
}
