package nsmc

import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class ReadTests extends FlatSpec with Matchers {

  val mongosHost = "localhost"
  val mongosPort = "27033"

  val mongodHost = "localhost"
  val mongodPort = "27030"

  val basicDB = "test"
  val basicCollection = "simple"
  val indexedCollection = "simple"

  val shardedDB = "shardedtest"
  val shardedCollection = "one"

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
        .set("nsmc.connection.host", mongodHost)
        .set("nsmc.connection.port", mongodPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection[MongoDBObject](basicDB, basicCollection)

    data.count() should be (300000)
    data.getPartitions.length should be (1)
    sc.stop()
  }

  "an unsharded, indexed collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", mongodHost)
        .set("nsmc.connection.port", mongodPort)
        .set("nsmc.split.indexed.collections", "true")
    val sc = new SparkContext(conf)
    val indexedKays = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](basicDB, indexedCollection, indexedKays)

    data.count() should be (300000)
    data.getPartitions.length should be (7)
    sc.stop()
  }

  "with direct shard access enabled, a sharded collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", mongosHost)
        .set("nsmc.connection.port", mongosPort)
        .set("nsmc.partition.on.shard.chunks", "true")
        .set("nsmc.direct.to.shards", "true")
    val sc = new SparkContext(conf)
    val indexedKays = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](shardedDB, shardedCollection, indexedKays)

    data.count() should be (400000)
    data.getPartitions.length should be (9)
    sc.stop()
  }

  "with direct shard access disabled, a sharded collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", mongosHost)
        .set("nsmc.connection.port", mongosPort)
        .set("nsmc.partition.on.shard.chunks", "true")
        .set("nsmc.direct.to.shards", "false")
    val sc = new SparkContext(conf)
    val indexedKays = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](shardedDB, shardedCollection, indexedKays)

    data.count() should be (400000)
    data.getPartitions.length should be (9)
    sc.stop()
  }

  "with shard chunks disabled, a sharded collection" should "get one partition" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", mongosHost)
        .set("nsmc.connection.port", mongosPort)
        .set("nsmc.partition.on.shard.chunks", "false")
        .set("nsmc.direct.to.shards", "false")
    val sc = new SparkContext(conf)
    val indexedKays = Seq("key")
    val data = sc.mongoCollection[MongoDBObject](shardedDB, shardedCollection, indexedKays)

    data.count() should be (400000)
    data.getPartitions.length should be (1)
    sc.stop()
  }

}
