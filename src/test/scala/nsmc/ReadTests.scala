package nsmc

import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkException, SparkContext, SparkConf}
import org.scalatest._



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

  "an authenticated user with read permissions" should "be able to read a db requiring auth" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("nsmc.connection.port", TestConfig.mongodAuthPort)
        .set("nsmc.user", "reader")
        .set("nsmc.password", "password")
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection[MongoDBObject](TestConfig.authDB,TestConfig.authCollection)

    data.count() should be (1000)
    data.getPartitions.length should be (1)
    sc.stop()
  }



  "a non-existent user" should "not be able to read a db requiring auth" in {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("nsmc.connection.port", TestConfig.mongodAuthPort)
        .set("nsmc.user", "nobody")
        .set("nsmc.password", "password")
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection[MongoDBObject](TestConfig.authDB,TestConfig.authCollection)

    a [SparkException] should be thrownBy {
      data.count()
    }
    sc.stop()

  }

  "a un-authenticated user" should "not be able to read a db requiring auth" in {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("nsmc.connection.port", TestConfig.mongodAuthPort)
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection[MongoDBObject](TestConfig.authDB,TestConfig.authCollection)

    a [SparkException] should be thrownBy {
      data.count()
    }
    sc.stop()

  }

  "a user without read permission on this database" should "not be able to read" in {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodAuthHost)
        .set("nsmc.connection.port", TestConfig.mongodAuthPort)
        .set("nsmc.user", "noroles")
        .set("nsmc.password", "password")
    val sc = new SparkContext(conf)
    val data = sc.mongoCollection[MongoDBObject](TestConfig.authDB,TestConfig.authCollection)

    a [SparkException] should be thrownBy {
      data.count()
    }
    sc.stop()

  }

}
