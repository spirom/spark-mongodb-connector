package nsmc.sql

import nsmc.TestConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.{StringType, IntegerType, StructField}
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
}
