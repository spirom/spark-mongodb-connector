package nsmc

import com.mongodb.casbah.Imports._
import nsmc.mongo.Conversions
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.StructField
import org.scalatest._

class MongoToSchemaRDDTests extends FlatSpec with Matchers {

  "a flat object" should "have a flat type" in {

    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> 99)

    val t = Conversions.toSchema(mo)

    t.size should be (2)
    t(0) should be (new StructField("key", StringType, false))
    t(1) should be (new StructField("val", IntegerType, false))

  }

  "a nested object" should "have a nested type" in {

    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> (MongoDBObject("a" -> 11) ++ ("b" -> 22)))

    val t = Conversions.toSchema(mo)

    t.size should be (2)
    t(0) should be (new StructField("key", StringType, false))
    //t(1) should be (new StructField("val", StructType(st), false))



  }
}
