package nsmc

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._

object TestConfig {

  val unknownHost = "unknown"
  val unknownPort = "27040"

  val mongosHost = "localhost"
  val mongosPort = "27033"

  val mongodHost = "localhost"
  val mongodPort = "27030"

  val mongodAuthHost = "localhost"
  val mongodAuthPort = "27034"

  val basicDB = "test"
  val basicCollection = "simple"
  val indexedCollection = "simple"

  val shardedDB = "shardedtest"
  val shardedCollection = "one"

  val authDB = "withauth"
  val authCollection = "authcoll"

  def makAuthCollection(): Unit = {
    val server = new ServerAddress(mongodAuthHost, mongodAuthPort.toInt)
    val credentials = MongoCredential.createMongoCRCredential("admin", authDB, "password".toCharArray)
    val mongoClient = MongoClient(server, List(credentials))

    val db = mongoClient.getDB(authDB)
    if (db.collectionExists(authCollection)) {
      db(authCollection).drop()
    }
    val col = db(authCollection)
    for (i <- 1 to 1000)
    {
      col += MongoDBObject("key" -> 1) ++ ("name" -> ("K_" + i))
    }
  }

  def main (args: Array[String]) {
    makAuthCollection()
  }
}