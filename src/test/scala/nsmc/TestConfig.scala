package nsmc

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._

object TestConfig {

  val unknownHost = "unknown"
  val unknownPort = "27040"

  val mongosHost = "dungeon"
  val mongosPort = "27033"

  val mongodHost = "dungeon"
  val mongodPort = "27030"

  val mongodAuthHost = "dungeon"
  val mongodAuthPort = "27034"

  val basicDB = "test"
  val basicCollection = "simple"
  val indexedCollection = "simple"
  val doubleIndexedCollection = "double"
  val scratchCollection = "scratch"

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
    for (i <- 1 to 1000) {
      col += MongoDBObject("key" -> 1) ++ ("name" -> ("K_" + i))
    }
  }

  def makeDoubleIndexedCollection(): Unit = {
    val mongoClient = MongoClient(TestConfig.mongodHost, TestConfig.mongodPort.toInt)

    val db = mongoClient.getDB(basicDB)
    if (db.collectionExists(doubleIndexedCollection)) {
      db(doubleIndexedCollection).drop()
    }
    val col = db(doubleIndexedCollection)

    for (i <- 1 to 300000) {
      col += MongoDBObject("key" -> i) ++ ("s" -> ("K_" + i))
    }

    col.ensureIndex(DBObject("key" -> 1, "s" -> -1))
  }

  def main(args: Array[String]) {
    makeDoubleIndexedCollection()
  }
}