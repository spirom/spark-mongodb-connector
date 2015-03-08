package nsmc.sql

import com.mongodb.casbah.Imports._
import nsmc.TestConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

class InsertTests extends FlatSpec with Matchers {
  "inserting into a populated table" should "should produce the right table" in {

    val mongoClient = MongoClient(TestConfig.mongodHost, TestConfig.mongodPort.toInt)
    val db = mongoClient.getDB("test")

    try {
      val col = db(TestConfig.scratchCollection)
      col.drop()
      col += MongoDBObject("key" -> 0) ++ ("s" -> "V0")
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
        |CREATE TEMPORARY TABLE fromTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE toTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.scratchCollection}')
      """.stripMargin)

      sqlContext.sql(
        s"""
        |INSERT INTO TABLE toTable SELECT _id, key, s FROM fromTable
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM toTable")

      val fields = data.schema.fields
      fields should have size (3)
      fields(0) should be (new StructField("_id", StringType, true))
      fields(1) should be (new StructField("key", IntegerType, true))
      fields(2) should be (new StructField("s", StringType, true))


      data.count() should be(300000)
      val firstRec = data.first()

      firstRec.size should be (3)
      // don't match the id
      firstRec.getString(1) should be ("V1")
      firstRec.getInt(2) should be (1)


    } finally {
      sc.stop()
    }
  }

  // TODO: test insertion not allowed (needs config)

  // TODO: test mismatched schemas
}
