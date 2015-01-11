package nsmc.conversion

import com.mongodb.casbah.Imports._
import org.apache.spark.sql.catalyst.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest._

import scala.collection.mutable

class InternalToSchemaTests extends FlatSpec with Matchers {

  "a flat object" should "have a flat type" in {

    val hm = new mutable.HashMap[String, ConversionType]
    hm += "key" -> new AtomicType(StringType)
    hm += "val" -> new AtomicType(IntegerType)

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t.size should be (2)
    t(0) should be (new StructField("key", StringType, false))
    //t(1) should be (new StructField("val", IntegerType, false))

  }

  "a nested object" should "have a nested type" in {

    val ihm = new mutable.HashMap[String, ConversionType]
    ihm += "a" -> new AtomicType(IntegerType)
    ihm += "b" -> new AtomicType(IntegerType)

    val hm = new mutable.HashMap[String, ConversionType]
    hm += "key" -> new AtomicType(StringType)
    hm += "val" -> new StructureType(ihm)

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t.size should be (2)
    t(0) should be (new StructField("key", StringType, false))
    val struct = t(2)
    struct shouldBe a [StructField]
    struct match {
      case StructField(k, dt, _) => {
        k should be ("val")
        dt shouldBe a [StructType]
        // more to do here
      }
      case _ => fail("can't happen")
    }

  }
}
