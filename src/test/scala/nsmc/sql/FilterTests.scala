package nsmc.sql

import nsmc.TestConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

class FilterTests extends FlatSpec with Matchers {
  "a single equality filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT s FROM dataTable WHERE key = 5")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("s", StringType, true))

      data.count() should be(1)
      val firstRec = data.first()

      firstRec.size should be (1)
      firstRec.getString(0) should be ("V5")

    } finally {
      sc.stop()
    }
  }

  "a single > filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT s FROM dataTable WHERE key > 299995")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("s", StringType, true))

      data.count() should be (5)
      val firstRec = data.first()

      firstRec.size should be (1)
      firstRec.getString(0) should be ("V299996")

    } finally {
      sc.stop()
    }
  }

  "a single >= filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT s FROM dataTable WHERE key >= 299995")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("s", StringType, true))

      data.count() should be (6)
      val firstRec = data.first()

      firstRec.size should be (1)
      firstRec.getString(0) should be ("V299995")

    } finally {
      sc.stop()
    }
  }

  "a single < filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT s FROM dataTable WHERE key < 5")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("s", StringType, true))

      data.count() should be (4)
      val recs = data.collect()

      recs(0).size should be (1)
      recs(0).getString(0) should be ("V1")

      recs(3).size should be (1)
      recs(3).getString(0) should be ("V4")

    } finally {
      sc.stop()
    }
  }

  "a single <= filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT s FROM dataTable WHERE key <= 5")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("s", StringType, true))

      data.count() should be (5)
      val recs = data.collect()

      recs(0).size should be (1)
      recs(0).getString(0) should be ("V1")

      recs(4).size should be (1)
      recs(4).getString(0) should be ("V5")

    } finally {
      sc.stop()
    }
  }

  "a single IN filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT s FROM dataTable WHERE key IN (10, 20, 30)")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("s", StringType, true))

      data.count() should be(3)
      val recs = data.collect()

      recs(0).size should be (1)
      recs(0).getString(0) should be ("V10")

      recs(1).size should be (1)
      recs(1).getString(0) should be ("V20")

      recs(2).size should be (1)
      recs(2).getString(0) should be ("V30")


    } finally {
      sc.stop()
    }
  }

  "multiple filters on the same columns" should "produce the right rows" in {
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
        sqlContext.sql("SELECT s FROM dataTable WHERE key > 7 AND key <= 10")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("s", StringType, true))

      data.count() should be (3)
      val recs = data.collect()

      recs(0).size should be (1)
      recs(0).getString(0) should be ("V8")

      recs(2).size should be (1)
      recs(2).getString(0) should be ("V10")

    } finally {
      sc.stop()
    }
  }

  "a single string-based IN filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT key FROM dataTable WHERE s IN ('V10', 'V20', 'V30')")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("key", IntegerType, true))

      data.count() should be(3)
      val recs = data.collect()

      recs(0).size should be (1)
      recs(0).getInt(0) should be (10)

      recs(1).size should be (1)
      recs(1).getInt(0) should be (20)

      recs(2).size should be (1)
      recs(2).getInt(0) should be (30)


    } finally {
      sc.stop()
    }
  }

  "a single string-based < filter" should "produce the right rows" in {
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
        sqlContext.sql("SELECT key FROM dataTable WHERE s < 'V2'")

      val fields = data.schema.fields
      fields should have size (1)
      fields(0) should be (new StructField("key", IntegerType, true))

      data.count() should be (111111)
      val recs = data.collect()

      recs(0).size should be (1)
      recs(0).getInt(0) should be (1)

      recs(111110).size should be (1)
      recs(111110).getInt(0) should be (199999)

    } finally {
      sc.stop()
    }
  }

  // TODO: test unknown column
}
