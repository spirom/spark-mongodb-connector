package nsmc.conversion

import nsmc.conversion.types._

import org.apache.spark.sql.types._

import org.scalatest.{Matchers, FlatSpec}

import scala.collection.immutable.HashMap

class MergeInternalTests extends FlatSpec with Matchers {

  "flat types without field overlap" should "merge correctly" in {
    val t1 = HashMap[String, ConversionType](
      "a" -> new nsmc.conversion.types.AtomicType(StringType),
      "b" -> new nsmc.conversion.types.AtomicType(IntegerType)
    )

    val t2 = HashMap[String, ConversionType](
      "c" -> new nsmc.conversion.types.AtomicType(StringType),
      "d" -> new nsmc.conversion.types.AtomicType(IntegerType)
    )

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (4)
    hm("a") should be (new nsmc.conversion.types.AtomicType(StringType))
    hm("b") should be (new nsmc.conversion.types.AtomicType(IntegerType))
    hm("c") should be (new nsmc.conversion.types.AtomicType(StringType))
    hm("d") should be (new nsmc.conversion.types.AtomicType(IntegerType))
  }

  "flat types with partial equal type overlap" should "merge correctly" in {
    val t1 = HashMap[String, ConversionType](
      "a" -> new nsmc.conversion.types.AtomicType(StringType),
      "b" -> new nsmc.conversion.types.AtomicType(IntegerType)
    )

    val t2 = HashMap[String, ConversionType](
      "b" -> new nsmc.conversion.types.AtomicType(IntegerType),
      "c" -> new nsmc.conversion.types.AtomicType(StringType)
    )

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (3)
    hm("a") should be (new nsmc.conversion.types.AtomicType(StringType))
    hm("b") should be (new nsmc.conversion.types.AtomicType(IntegerType))
    hm("c") should be (new nsmc.conversion.types.AtomicType(StringType))
  }

  "flat types with full equal type overlap" should "merge correctly" in {
    val t1 = HashMap[String, ConversionType](
      "a" -> new nsmc.conversion.types.AtomicType(StringType),
      "b" -> new nsmc.conversion.types.AtomicType(IntegerType)
    )

    val t2 = HashMap[String, ConversionType](
      "a" -> new nsmc.conversion.types.AtomicType(StringType),
      "b" -> new nsmc.conversion.types.AtomicType(IntegerType)
    )

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (2)
    hm("a") should be (new nsmc.conversion.types.AtomicType(StringType))
    hm("b") should be (new nsmc.conversion.types.AtomicType(IntegerType))
  }

  "flat types with incompatibly typed overlaps" should "merge correctly" ignore {

  }

  "nested types with overlaps" should "merge correctly" in {

    val inner1 = HashMap[String, ConversionType](
      "x" -> new nsmc.conversion.types.AtomicType(StringType),
      "y" -> new nsmc.conversion.types.AtomicType(IntegerType)
    )

    val t1 = HashMap[String, ConversionType](
      "a" -> new nsmc.conversion.types.AtomicType(StringType),
      "b" -> new StructureType(inner1)
    )

    val inner2 = HashMap[String, ConversionType](
      "y" -> new nsmc.conversion.types.AtomicType(IntegerType),
      "z" -> new nsmc.conversion.types.AtomicType(StringType)
    )

    val t2 = HashMap[String, ConversionType](
      "b" -> new StructureType(inner2),
      "c" -> new nsmc.conversion.types.AtomicType(StringType)
    )

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (3)
    hm("a") should be (new nsmc.conversion.types.AtomicType(StringType))
    hm("b") shouldBe a [StructureType]
    val inner = hm("b").asInstanceOf[StructureType].fields
    inner should have size 3
    inner("x") should be (new nsmc.conversion.types.AtomicType(StringType))
    inner("y") should be (new nsmc.conversion.types.AtomicType(IntegerType))
    inner("z") should be (new nsmc.conversion.types.AtomicType(StringType))
    hm("c") should be (new nsmc.conversion.types.AtomicType(StringType))
  }

  "atomic array types" should "merge correctly" ignore {

  }

  "structured array types" should "merge correctly" ignore {

  }

  "nested array types" should "merge correctly" ignore {

  }
}
