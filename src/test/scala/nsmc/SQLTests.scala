package nsmc

import com.mongodb.casbah.Imports._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}


class SQLTests extends FlatSpec with Matchers {
  "collection with flat schema and fully populated fields" should "query correctly with *" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodHost)
        .set("nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM dataTable")

      data.count() should be(300000)
      println(data.first())
      data.schema.printTreeString()

      //val lessdata =
      //  sqlContext.sql("SELECT * FROM dataTable WHERE key < 10")

     // lessdata.count() should be(9)

    } finally {
      sc.stop()
    }
  }

  it should "select columns correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodHost)
        .set("nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT s, key FROM dataTable")

      val firstRec = data.first()
      firstRec.getString(0) should be ("V1")
      firstRec.getInt(1) should be (1)

    } finally {
      sc.stop()
    }
  }

  it should "filter correctly" in {
    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodHost)
        .set("nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT s FROM dataTable WHERE key > 299900")

      data.count() should be (100)

      val firstRec = data.first()
      firstRec.getString(0) should be ("V299901")

    } finally {
      sc.stop()
    }
  }

  "collection with flat schema and partially populated fields" should "query correctly" in {
    val mongoClient = MongoClient(TestConfig.mongodHost, TestConfig.mongodPort.toInt)


    val db = mongoClient.getDB("test")

    try {
      val col = db(TestConfig.scratchCollection)
      col.drop()
      col += MongoDBObject("f1" -> 1)
      col += MongoDBObject("f1" -> 2) ++ ("f2" -> "S2") ++ ("f3" -> 102)
      col += MongoDBObject("f3" -> 202)
      col += MongoDBObject("f1" -> 4) ++ ("f2" -> "S4")
    } finally {
      mongoClient.close()
    }

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", TestConfig.mongodHost)
        .set("nsmc.connection.port", TestConfig.mongodPort)
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {

      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.scratchCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT f1 FROM dataTable WHERE f1 IS NOT NULL")

      val ttt = data.schema


      data.count() should be (2)

      val firstRec = data.first()
      firstRec.getInt(0) should be (2)

    } finally {
      sc.stop()
    }

  }

  "collection with nested schema and fully populated fields" should "query correctly" in {

  }

  "collection with nested schema and partially populated fields" should "query correctly" in {

  }
}
