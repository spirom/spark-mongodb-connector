package nsmc

import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

object TestConfig {
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
    //if (db.collectionExists(authCollection)) {
    //  db(authCollection).drop()
    //}
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

class ReadTests extends FlatSpec with Matchers {



  "an unreachable server" should "fail gracefully" in {

  }

  "an unknown database" should "fail gracefully" in {

  }


  "an unknown collection" should "fail gracefully" in {

  }

  "an unindexed collection" should "get a single partition" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodHost)
        .set("nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection[MongoDBObject](TestConfig.basicDB,TestConfig.basicCollection)

    data.count() should be (300000)
    data.getPartitions.length should be (1)
    sc.stop()
  }

  "an unsharded, indexed collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodHost)
        .set("nsmc.connection.port", TestConfig.mongodPort)
        .set("nsmc.split.indexed.collections", "true")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](TestConfig.basicDB,
      TestConfig.indexedCollection, indexedKeys)

    data.count() should be (300000)
    data.getPartitions.length should be (7)
    sc.stop()
  }

  "with direct shard access enabled, a sharded collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongosHost)
        .set("nsmc.connection.port", TestConfig.mongosPort)
        .set("nsmc.partition.on.shard.chunks", "true")
        .set("nsmc.direct.to.shards", "true")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](TestConfig.shardedDB,
      TestConfig.shardedCollection, indexedKeys)

    data.count() should be (400000)
    data.getPartitions.length should be (9)
    sc.stop()
  }

  "with direct shard access disabled, a sharded collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongosHost)
        .set("nsmc.connection.port", TestConfig.mongosPort)
        .set("nsmc.partition.on.shard.chunks", "true")
        .set("nsmc.direct.to.shards", "false")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](TestConfig.shardedDB,
      TestConfig.shardedCollection, indexedKeys)

    data.count() should be (400000)
    data.getPartitions.length should be (9)
    sc.stop()
  }

  "with shard chunks disabled, a sharded collection" should "get one partition" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongosHost)
        .set("nsmc.connection.port", TestConfig.mongosPort)
        .set("nsmc.partition.on.shard.chunks", "false")
        .set("nsmc.direct.to.shards", "false")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](TestConfig.shardedDB,
      TestConfig.shardedCollection, indexedKeys)

    data.count() should be (400000)
    data.getPartitions.length should be (1)
    sc.stop()
  }

}
