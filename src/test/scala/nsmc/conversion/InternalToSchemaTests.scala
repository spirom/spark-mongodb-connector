package nsmc.conversion

import nsmc.conversion.types.{InternalAndSchema, StructureType, AtomicType, ConversionType}

import org.apache.spark.sql.catalyst.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest._

import scala.collection.immutable.HashMap

class InternalToSchemaTests extends FlatSpec with Matchers {

  "a flat object" should "have a flat type" in {

    val hm = HashMap[String, ConversionType](
      "key" -> new AtomicType(StringType),
      "val" -> new AtomicType(IntegerType)
    )

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t.size should be (2)
    t(0) should be (new StructField("key", StringType, true))
    t(1) should be (new StructField("val", IntegerType, true))

  }

  "a nested object" should "have a nested type" in {

    val ihm = HashMap[String, ConversionType](
      "a" -> new AtomicType(IntegerType),
      "b" -> new AtomicType(IntegerType),
      "c" -> new AtomicType(StringType)
    )

    val hm = HashMap[String, ConversionType](
      "key" -> new AtomicType(StringType),
      "val" -> new StructureType(ihm)
    )

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t.size should be (2)
    t(0) should be (new StructField("key", StringType, true))
    val struct = t(1)
    struct shouldBe a [StructField]
    struct match {
      case StructField(k, dt, _, _) => {
        k should be ("val")
        dt shouldBe a [StructType]
        val innerFields = dt.asInstanceOf[StructType].fields
        innerFields.size should be (3)
        innerFields(0) should be (new StructField("a", IntegerType, true))
        innerFields(1) should be (new StructField("b", IntegerType, true))
        innerFields(2) should be (new StructField("c", StringType, true))
      }
      case _ => fail("can't happen")
    }

  }
}
