package nsmc

import com.mongodb.casbah.Imports._
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._

class MongoToSchemaRDDTests extends FlatSpec with Matchers {

  "an unknown collection" should "fail gracefully" in {
    val conf =
      new SparkConf()
        .setAppName("Converter").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    case class CC(i: Int)

    val mo = MongoDBObject("key" -> "hello") ++ ("" -> 99)

    //val (t, o) = toSchema(mo)
  }
}
