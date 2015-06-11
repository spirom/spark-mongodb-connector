package nsmc.conversion

import nsmc.conversion.types._

import org.apache.spark.sql.types._
import org.scalatest._

import scala.collection.immutable.HashMap

class InternalToSchemaTests extends FlatSpec with Matchers {

  "a flat object" should "have a flat type" in {

    val hm = HashMap[String, ConversionType](
      "key" -> new nsmc.conversion.types.AtomicType(StringType),
      "val" -> new nsmc.conversion.types.AtomicType(IntegerType)
    )

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t shouldBe a [StructType]
    val st = t.asInstanceOf[StructType].fields

    st.size should be (2)
    st(0) should be (new StructField("key", StringType, true))
    st(1) should be (new StructField("val", IntegerType, true))

  }

  "a nested object" should "have a nested type" in {

    val ihm = HashMap[String, ConversionType](
      "a" -> new nsmc.conversion.types.AtomicType(IntegerType),
      "b" -> new nsmc.conversion.types.AtomicType(IntegerType),
      "c" -> new nsmc.conversion.types.AtomicType(StringType)
    )

    val hm = HashMap[String, ConversionType](
      "key" -> new nsmc.conversion.types.AtomicType(StringType),
      "val" -> new StructureType(ihm)
    )

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t shouldBe a [StructType]
    val st = t.asInstanceOf[StructType].fields

    st.size should be (2)
    st(0) should be (new StructField("key", StringType, true))
    val struct = st(1)
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

  "an atomic array object" should "have an atomic array type" in {
    val hm = HashMap[String, ConversionType](
      "key" -> new SequenceType(new nsmc.conversion.types.AtomicType(IntegerType))
    )

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t shouldBe a [StructType]
    val st = t.asInstanceOf[StructType].fields

    st.size should be (1)
    st(0) should be (new StructField("key", ArrayType(IntegerType), true))
  }

  "a nested atomic array object" should "have a nested atomic array type" in {
    val hm = HashMap[String, ConversionType](
      "key" -> new SequenceType(new SequenceType(new nsmc.conversion.types.AtomicType(StringType)))
    )

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t shouldBe a [StructType]
    val st = t.asInstanceOf[StructType].fields

    st.size should be (1)
    st(0) should be (new StructField("key", ArrayType(ArrayType(StringType)), true))
  }

  "an array of structures" should "have a array og structures type" in {
    val ihm = HashMap[String, ConversionType](
      "a" -> new nsmc.conversion.types.AtomicType(IntegerType),
      "b" -> new nsmc.conversion.types.AtomicType(IntegerType),
      "c" -> new nsmc.conversion.types.AtomicType(StringType)
    )

    val hm = HashMap[String, ConversionType](
      "key" -> new SequenceType(new StructureType(ihm))
    )

    val t = InternalAndSchema.toSchema(new StructureType(hm))

    t shouldBe a [StructType]
    val st = t.asInstanceOf[StructType].fields

    st.size should be (1)
    val struct = st(0)
    struct shouldBe a [StructField]
    struct match {
      case StructField(k, dt, _, _) => {
        k should be ("key")
        dt shouldBe a [ArrayType]
        val inner = dt.asInstanceOf[ArrayType].elementType
        inner shouldBe a [StructType]
        val innerFields = inner.asInstanceOf[StructType].fields
        innerFields.size should be (3)
        innerFields(0) should be (new StructField("a", IntegerType, true))
        innerFields(1) should be (new StructField("b", IntegerType, true))
        innerFields(2) should be (new StructField("c", StringType, true))
      }
      case _ => fail("can't happen")
    }
  }

}
