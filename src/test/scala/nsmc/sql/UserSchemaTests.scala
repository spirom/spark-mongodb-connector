package nsmc.sql

import nsmc.TestConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

class UserSchemaTests extends FlatSpec with Matchers {
  "collection with matching, flat user-specified schema" should "query correctly with *" in {
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
        |CREATE TEMPORARY TABLE dataTable (_id string, key int, s string)
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM dataTable")

      val fields = data.schema.fields
      fields should have size (3)
      fields(0) should be (new StructField("_id", StringType, true))
      fields(1) should be (new StructField("key", IntegerType, true))
      fields(2) should be (new StructField("s", StringType, true))

      data.count() should be(300000)
      val firstRec = data.first()

      // don't match the id
      firstRec.getInt(1) should be (1)
      firstRec.getString(2) should be ("V1")

    } finally {
      sc.stop()
    }
  }

  "collection with incomplete user-specified schema" should "return specified columns when queried with *" in {
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
        |CREATE TEMPORARY TABLE dataTable (_id string, s string)
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${TestConfig.basicDB}', collection '${TestConfig.basicCollection}')
      """.stripMargin)

      val data =
        sqlContext.sql("SELECT * FROM dataTable")

      val fields = data.schema.fields
      fields should have size (2)
      fields(0) should be (new StructField("_id", StringType, true))
      fields(1) should be (new StructField("s", StringType, true))

      data.count() should be(300000)
      val firstRec = data.first()

      // don't match the id
      firstRec.getString(1) should be ("V1")

    } finally {
      sc.stop()
    }
  }
}
