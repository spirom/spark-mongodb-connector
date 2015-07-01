package nsmc.sql

import com.mongodb.casbah.Imports._
import nsmc.TestConfig
import org.apache.spark.sql.sources._
import org.scalatest.{Matchers, FlatSpec}


class QueryGeneratorTests extends FlatSpec with Matchers {

  trait Builder {
    val mongoClient = MongoClient(TestConfig.mongodHost, TestConfig.mongodPort.toInt)

    val col = try {
      val db = mongoClient.getDB("test")

      val col = db(TestConfig.scratchCollection)
      col.drop()
      col += MongoDBObject("f1" -> 1) ++ ("f2" -> 1.0) ++ ("f3" -> "S1") ++ ("f4" -> 1)
      col += MongoDBObject("f1" -> 2) ++ ("f2" -> 2.0) ++ ("f3" -> "S2") ++ ("f4" -> null)
      col += MongoDBObject("f1" -> 3) ++ ("f2" -> 3.0) ++ ("f3" -> "S3")
      col += MongoDBObject("f1" -> 4) ++ ("f2" -> 4.0) ++ ("f3" -> "S4") ++ ("f4" -> 4)
      col += MongoDBObject("f1" -> 5) ++ ("f2" -> 5.0) ++ ("f3" -> "S5") ++ ("f4" -> 5)
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

  "projecting one column with an equality filter" should
    "yield one column in one row" in new Builder {

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

  "projecting one column with an 'in' filter" should
    "yield one column in two rows" in new Builder {

    try {

      val filters : Array[Filter] = Array(In("f1", Array(2, 4)))
      val columns = Array("f1")

      val results = query(filters, columns)

      results.size should be (2)
      results(0).keySet().size() should be (1)
      results(0).get("f1") should be (2)
      results(1).keySet().size() should be (1)
      results(1).get("f1") should be (4)

    } finally {
      close()
    }

  }

  "projecting one column with a '>' filter" should
    "yield one column in two rows" in new Builder {

    try {

      val filters : Array[Filter] = Array(GreaterThan("f1", 3))
      val columns = Array("f1")

      val results = query(filters, columns)

      results.size should be (2)
      results(0).keySet().size() should be (1)
      results(0).get("f1") should be (4)
      results(1).keySet().size() should be (1)
      results(1).get("f1") should be (5)

    } finally {
      close()
    }

  }

  "projecting one column with a '>=' filter" should
    "yield one column in three rows" in new Builder {

    try {

      val filters : Array[Filter] = Array(GreaterThanOrEqual("f1", 3))
      val columns = Array("f1")

      val results = query(filters, columns)

      results.size should be (3)
      results(0).keySet().size() should be (1)
      results(0).get("f1") should be (3)
      results(1).keySet().size() should be (1)
      results(1).get("f1") should be (4)
      results(2).keySet().size() should be (1)
      results(2).get("f1") should be (5)

    } finally {
      close()
    }

  }

  "projecting one column with a '<' filter" should
    "yield one column in two rows" in new Builder {

    try {

      val filters : Array[Filter] = Array(LessThan("f1", 3))
      val columns = Array("f1")

      val results = query(filters, columns)

      results.size should be (2)
      results(0).keySet().size() should be (1)
      results(0).get("f1") should be (1)
      results(1).keySet().size() should be (1)
      results(1).get("f1") should be (2)

    } finally {
      close()
    }

  }

  "projecting one column with a '<=' filter" should
    "yield one column in three rows" in new Builder {

    try {

      val filters : Array[Filter] = Array(LessThanOrEqual("f1", 3))
      val columns = Array("f1")

      val results = query(filters, columns)

      results.size should be (3)
      results(0).keySet().size() should be (1)
      results(0).get("f1") should be (1)
      results(1).keySet().size() should be (1)
      results(1).get("f1") should be (2)
      results(2).keySet().size() should be (1)
      results(2).get("f1") should be (3)

    } finally {
      close()
    }

  }

  "projecting two columns with a null filter" should
    "yield two rows with the right number of fields" in new Builder {

    try {

      val filters : Array[Filter] = Array(IsNull("f4"))
      val columns = Array("f1", "f4")

      val results = query(filters, columns)

      results.size should be (2)
      results(0).keySet().size() should be (2)
      results(0).get("f1") should be (2)
      results(1).keySet().size() should be (1)
      results(1).get("f1") should be (3)


    } finally {
      close()
    }

  }

  "projecting two columns with an And filter" should
    "yields one row with the right fields" in new Builder {

    try {

      val filters : Array[Filter] = Array(And(LessThan("f1", 4), GreaterThan("f2", 2.0)))
      val columns = Array("f1", "f2")

      val results = query(filters, columns)

      results.size should be (1)
      results(0).keySet().size() should be (2)
      results(0).get("f1") should be (3)
      results(0).get("f2") should be (3.0)

    } finally {
      close()
    }

  }

  "projecting two columns with an Or filter" should
    "yields two row with the right fields" in new Builder {

    try {

      val filters : Array[Filter] = Array(Or(LessThan("f1", 2), GreaterThan("f2", 4.0)))
      val columns = Array("f1", "f2")

      val results = query(filters, columns)

      results.size should be (2)
      results(0).keySet().size() should be (2)
      results(0).get("f1") should be (1)
      results(0).get("f2") should be (1.0)
      results(1).keySet().size() should be (2)
      results(1).get("f1") should be (5)
      results(1).get("f2") should be (5.0)

    } finally {
      close()
    }

  }

}
