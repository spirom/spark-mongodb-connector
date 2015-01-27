package nsmc.conversion

import com.mongodb.casbah.Imports._
import org.apache.spark.sql.catalyst.types._
import org.scalatest._

class MongoToInternalTests extends FlatSpec with Matchers {

  "a flat object" should "have a flat type" in {

    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> 99)

    val t = MongoAndInternal.toInternal(mo)

    t.fields.size should be (2)
    t.fields("key") should be (new AtomicType(StringType))
    t.fields("val") should be (new AtomicType(IntegerType))
  }

  "a nested object" should "have a nested type" in {

    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> (MongoDBObject("a" -> 11) ++ ("b" -> 22)))

    val t = MongoAndInternal.toInternal(mo)

    t.fields.size should be (2)
    t.fields("key") should be (new AtomicType(StringType))
    val struct = t.fields("val")
    struct shouldBe a [StructureType]
    struct match {
      case StructureType(fields) => {
        fields.size should be (2)
        fields("a") should be (new AtomicType(IntegerType))
        fields("b") should be (new AtomicType(IntegerType))
      }
      case _ => fail("can't happen")
    }
  }

  // TODO: arrays


}
