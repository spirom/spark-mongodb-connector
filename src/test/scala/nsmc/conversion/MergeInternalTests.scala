package nsmc.conversion

import nsmc.conversion.types._

import org.apache.spark.sql.catalyst.types.{IntegerType, StringType}
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable

class MergeInternalTests extends FlatSpec with Matchers {

  "flat types without field overlap" should "merge correctly" in {
    val t1 = new mutable.HashMap[String, ConversionType]
    t1 += "a" -> new AtomicType(StringType)
    t1 += "b" -> new AtomicType(IntegerType)

    val t2 = new mutable.HashMap[String, ConversionType]
    t2 += "c" -> new AtomicType(StringType)
    t2 += "d" -> new AtomicType(IntegerType)

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (4)
    hm("a") should be (new AtomicType(StringType))
    hm("b") should be (new AtomicType(IntegerType))
    hm("c") should be (new AtomicType(StringType))
    hm("d") should be (new AtomicType(IntegerType))
  }

  "flat types with partial equal type overlap" should "merge correctly" in {
    val t1 = new mutable.HashMap[String, ConversionType]
    t1 += "a" -> new AtomicType(StringType)
    t1 += "b" -> new AtomicType(IntegerType)

    val t2 = new mutable.HashMap[String, ConversionType]
    t2 += "b" -> new AtomicType(IntegerType)
    t2 += "c" -> new AtomicType(StringType)

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (3)
    hm("a") should be (new AtomicType(StringType))
    hm("b") should be (new AtomicType(IntegerType))
    hm("c") should be (new AtomicType(StringType))
  }

  "flat types with full equal type overlap" should "merge correctly" in {
    val t1 = new mutable.HashMap[String, ConversionType]
    t1 += "a" -> new AtomicType(StringType)
    t1 += "b" -> new AtomicType(IntegerType)

    val t2 = new mutable.HashMap[String, ConversionType]
    t2 += "a" -> new AtomicType(StringType)
    t2 += "b" -> new AtomicType(IntegerType)

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (2)
    hm("a") should be (new AtomicType(StringType))
    hm("b") should be (new AtomicType(IntegerType))
  }

  "flat types with incompatibly typed overlaps" should "merge correctly" ignore {

  }

  "nested types with overlaps" should "merge correctly" in {

    val inner1 = new mutable.HashMap[String, ConversionType]
    inner1 += "x" -> new AtomicType(StringType)
    inner1 += "y" -> new AtomicType(IntegerType)

    val t1 = new mutable.HashMap[String, ConversionType]
    t1 += "a" -> new AtomicType(StringType)
    t1 += "b" -> new StructureType(inner1)

    val inner2 = new mutable.HashMap[String, ConversionType]
    inner2 += "y" -> new AtomicType(IntegerType)
    inner2 += "z" -> new AtomicType(StringType)

    val t2 = new mutable.HashMap[String, ConversionType]
    t2 += "b" -> new StructureType(inner2)
    t2 += "c" -> new AtomicType(StringType)

    val merged = Merger.merge(new StructureType(t1), new StructureType(t2))

    merged shouldBe a [StructureType]
    val hm = merged.asInstanceOf[StructureType].fields

    hm should have size (3)
    hm("a") should be (new AtomicType(StringType))
    hm("b") shouldBe a [StructureType]
    val inner = hm("b").asInstanceOf[StructureType].fields
    inner should have size 3
    inner("x") should be (new AtomicType(StringType))
    inner("y") should be (new AtomicType(IntegerType))
    inner("z") should be (new AtomicType(StringType))
    hm("c") should be (new AtomicType(StringType))
  }
}
