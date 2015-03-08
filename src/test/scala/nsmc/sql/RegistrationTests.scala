package nsmc.sql

import nsmc.TestConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}

class RegistrationTests extends FlatSpec with Matchers {
"a single registration" should "support multiple queries" in {
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

      val fields = data.schema.fields
      fields should have size (3)
      fields(0) should be (new StructField("_id", StringType, true))
      fields(1) should be (new StructField("key", IntegerType, true))
      fields(2) should be (new StructField("s", StringType, true))

      data.count() should be(300000)
      val firstRec = data.first()

      firstRec.size should be (3)
      // don't match the id
      firstRec.getInt(1) should be (1)
      firstRec.getString(2) should be ("V1")

      val data2 =
        sqlContext.sql("SELECT s FROM dataTable WHERE key = 5")

      val fields2 = data2.schema.fields
      fields2 should have size (1)
      fields2(0) should be (new StructField("s", StringType, true))

      data2.count() should be(1)
      val firstRec2 = data2.first()

      firstRec2.size should be (1)
      firstRec2.getString(0) should be ("V5")

    } finally {
      sc.stop()
    }
  }
}
