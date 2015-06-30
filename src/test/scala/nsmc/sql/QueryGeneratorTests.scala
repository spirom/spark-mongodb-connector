package nsmc.sql

import com.mongodb.casbah.Imports._
import nsmc.TestConfig
import org.apache.spark.sql.sources.{Filter, EqualTo}
import org.scalatest.{Matchers, FlatSpec}


class QueryGeneratorTests extends FlatSpec with Matchers {

  trait Builder {
    val mongoClient = MongoClient(TestConfig.mongodHost, TestConfig.mongodPort.toInt)

    val col = try {
      val db = mongoClient.getDB("test")

      val col = db(TestConfig.scratchCollection)
      col.drop()
      col += MongoDBObject("f1" -> 1) ++ ("f2" -> 1.0) ++ ("f3" -> "S1")
      col += MongoDBObject("f1" -> 2) ++ ("f2" -> 2.0) ++ ("f3" -> "S2")
      col += MongoDBObject("f1" -> 3) ++ ("f2" -> 3.0) ++ ("f3" -> "S3")
      col += MongoDBObject("f1" -> 4) ++ ("f2" -> 4.0) ++ ("f3" -> "S4")
      col += MongoDBObject("f1" -> 5) ++ ("f2" -> 5.0) ++ ("f3" -> "S5")
      col
    } catch {
      case e: Exception => {
        mongoClient.close()
        throw e
      }
    }

    def query(filters: Array[Filter], columns: Array[String]) = {
      val queryGenerator = new QueryGenerator

      val filter = queryGenerator.makeFilter(filters)
      val projection = queryGenerator.makeProjection(columns)
      val cursor = col.find(filter, projection)

      cursor.toArray
    }

    def close() : Unit = {
      mongoClient.close()
    }
  }

  "projecting one column without filtering" should "yield a full column" in new Builder {

    try {

      val filters : Array[Filter] = Array()
      val columns = Array("f1")

      val results = query(filters, columns)

      results.size should be (5)
      for (i <- 0 to 4) {
        results(i).keySet().size() should be(1)
        results(i).get("f1") should be(i+1)
      }

    } finally {
      close()
    }

  }

  "projecting two columns without filtering" should
    "yield two full columns in the right order" in new Builder {

    try {

      val filters : Array[Filter] = Array()
      val columns = Array("f3", "f1")

      val results = query(filters, columns)

      results.size should be (5)
      for (i <- 0 to 4) {
        results(i).keySet().size() should be(2)
        results(i).get("f3") should be(s"S${i+1}")
        results(i).get("f1") should be(i+1)
      }

    } finally {
      close()
    }

  }

  "projecting one column with an equality filter" should "yield one column in one row" in new Builder {

    try {

      val filters : Array[Filter] = Array(EqualTo("f1", 3))
      val columns = Array("f1")

      val results = query(filters, columns)

      results.size should be (1)
      results(0).keySet().size() should be (1)
      results(0).get("f1") should be (3)

    } finally {
      close()
    }

  }

  }
