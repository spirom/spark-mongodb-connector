package nsmc.mongo

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import org.apache.spark.SparkConf

class MongoConnector(conf: MongoConnectorConf) extends Serializable with nsmc.Logging {

  def getCollection(databaseName: String, collectionName: String): Iterator[MongoDBObject] = {
    logDebug(s"Obtaining data for collection '${collectionName}' in database '${databaseName}'")

    val server = new ServerAddress(conf.host, conf.port)
    val mongoClient = if (conf.user.isDefined && conf.password.isDefined) {
      val credentials = MongoCredential.createMongoCRCredential(conf.user.get, databaseName, conf.password.get.toCharArray)
      logDebug(s"Connecting with password for user '${conf.user.get}'")
      MongoClient(server, List(credentials))
    } else {
      logDebug(s"Connecting without credentials")
      MongoClient(server)
    }
    val db = mongoClient.getDB(databaseName)
    val col = db(collectionName)
    col.iterator.map(e => e)
  }
}

object MongoConnector extends nsmc.Logging {
  def apply(conf: SparkConf): MongoConnector = {
    new MongoConnector(MongoConnectorConf(conf))
  }

  // fetch data from the given interval
  def getCollection(databaseName: String, collectionName: String, interval: MongoInterval): Iterator[DBObject] = {
    logDebug(s"Obtaining interval data for collection '${collectionName}' in database '${databaseName}' at '${interval.destination.host}:${interval.destination.port}'")
    val server = new ServerAddress(interval.destination.host, interval.destination.port)
    val conf = interval.destination.conf
    val mongoClient = if (conf.user.isDefined && conf.password.isDefined) {
      val credentials = MongoCredential.createMongoCRCredential(conf.user.get, databaseName, conf.password.get.toCharArray)
      logDebug(s"Connecting with password for user '${conf.user.get}'")
      MongoClient(server, List(credentials))
    } else {
      logDebug(s"Connecting without credentials")
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
