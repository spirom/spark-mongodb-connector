package nsmc.sql

import com.mongodb.casbah.Imports._
import nsmc.TestConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.{IntegerType, StructField}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

class ProjectionTests extends FlatSpec with Matchers {
  "collection with flat schema and partially populated fields" should "be projected correctly" in {
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
        sqlContext.sql("SELECT f1, f3 FROM dataTable")

      val fields = data.schema.fields
      fields should have size (2)
      fields(0) should be (new StructField("f1", IntegerType, true))
      fields(1) should be (new StructField("f3", IntegerType, true))

      data.count() should be (4)

      val results = data.collect()

      results(0).getInt(0) should be (1)
      results(0).isNullAt(1) should be (true)

      results(1).getInt(0) should be (2)
      results(1).getInt(1) should be (102)

      results(2).isNullAt(0) should be (true)
      results(2).getInt(1) should be (202)

      results(3).getInt(0) should be (4)
      results(3).isNullAt(1) should be (true)

    } finally {
      sc.stop()
    }

  }
}
