package nsmc

import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkException, SparkContext, SparkConf}
import org.scalatest._



class RDDReadTests extends FlatSpec with Matchers {



  "an unreachable server" should "fail gracefully" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.unknownHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection("UnknownDB", TestConfig.basicCollection)

    a [SparkException] should be thrownBy {
      data.count() should be(0)
    }
    sc.stop()
  }

  "an unused server port" should "fail gracefully" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.unknownPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection("UnknownDB", TestConfig.basicCollection)

    a [SparkException] should be thrownBy {
      data.count() should be(0)
    }
    sc.stop()
  }

  "any collection in an unknown database" should "seem to be empty" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection("UnknownDB", TestConfig.basicCollection)

    data.count() should be (0)
    data.getPartitions.length should be (1)
    sc.stop()
  }


  "an unknown collection" should "fail seem to be empty" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection(TestConfig.basicDB, "UnknownColl")

    data.count() should be (0)
    data.getPartitions.length should be (1)
    sc.stop()
  }

  "an unindexed collection" should "get a single partition" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection[DBObject](TestConfig.basicDB,TestConfig.basicCollection)

    try {
      data.count() should be(300000)
      val c = data.collect()
      data.getPartitions.length should be(1)
    } finally {
      sc.stop()
    }
  }

  "an unsharded, indexed collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
        .set("spark.nsmc.split.indexed.collections", "true")
        .set("spark.nsmc.split.chunk.size", "4")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq(("key", 1))
    val data = sc.mongoCollection(TestConfig.basicDB,
      TestConfig.indexedCollection, indexedKeys)

    data.count() should be (300000)
    data.getPartitions.length should be (7)
    sc.stop()
  }

  "an unsharded, multiply indexed collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodPort)
        .set("spark.nsmc.split.indexed.collections", "true")
        .set("spark.nsmc.split.chunk.size", "4")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq(("key", 1), ("s", -1))
    val data = sc.mongoCollection(TestConfig.basicDB,
      TestConfig.doubleIndexedCollection, indexedKeys)

    data.count() should be (300000)
    data.getPartitions.length should be (7)
    sc.stop()
  }

  "with direct shard access enabled, a sharded collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongosHost)
        .set("spark.nsmc.connection.port", TestConfig.mongosPort)
        .set("spark.nsmc.partition.on.shard.chunks", "true")
        .set("spark.nsmc.direct.to.shards", "true")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq(("key",1))
    val data = sc.mongoCollection(TestConfig.shardedDB,
      TestConfig.shardedCollection, indexedKeys)

    data.count() should be (400000)
    data.getPartitions.length should be (9)
    sc.stop()
  }

  "with direct shard access disabled, a sharded collection" should "get partitioned correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongosHost)
        .set("spark.nsmc.connection.port", TestConfig.mongosPort)
        .set("spark.nsmc.partition.on.shard.chunks", "true")
        .set("spark.nsmc.direct.to.shards", "false")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq(("key",1))
    val data = sc.mongoCollection(TestConfig.shardedDB,
      TestConfig.shardedCollection, indexedKeys)

    data.count() should be (400000)
    data.getPartitions.length should be (9)
    sc.stop()
  }

  "with shard chunks disabled, a sharded collection" should "get one partition" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongosHost)
        .set("spark.nsmc.connection.port", TestConfig.mongosPort)
        .set("spark.nsmc.partition.on.shard.chunks", "false")
        .set("spark.nsmc.direct.to.shards", "false")
    val sc = new SparkContext(conf)
    val indexedKeys = Seq(("key",1))
    val data = sc.mongoCollection(TestConfig.shardedDB,
      TestConfig.shardedCollection, indexedKeys)

    data.count() should be (400000)
    data.getPartitions.length should be (1)
    sc.stop()
  }

  "an authenticated user with read permissions" should "be able to read a db requiring auth" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodAuthPort)
        .set("spark.nsmc.user", "reader")
        .set("spark.nsmc.password", "password")
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection(TestConfig.authDB,TestConfig.authCollection)

    data.count() should be (1000)
    data.getPartitions.length should be (1)
    sc.stop()
  }



  "a non-existent user" should "not be able to read a db requiring auth" in {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodAuthPort)
        .set("spark.nsmc.user", "nobody")
        .set("spark.nsmc.password", "password")
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection(TestConfig.authDB,TestConfig.authCollection)

    a [SparkException] should be thrownBy {
      data.count()
    }
    sc.stop()

  }

  "a un-authenticated user" should "not be able to read a db requiring auth" in {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodAuthPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection(TestConfig.authDB,TestConfig.authCollection)

    a [SparkException] should be thrownBy {
      data.count()
    }
    sc.stop()

  }

  "a user without read permission on this database" should "not be able to read" in {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("spark.nsmc.connection.port", TestConfig.mongodAuthPort)
        .set("spark.nsmc.user", "noroles")
        .set("spark.nsmc.password", "password")
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection(TestConfig.authDB,TestConfig.authCollection)

    a [SparkException] should be thrownBy {
      data.count()
    }
    sc.stop()

  }

}
