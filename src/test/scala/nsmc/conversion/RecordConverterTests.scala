package nsmc.conversion

import com.mongodb.casbah.Imports._
import nsmc.conversion.types.{StructureType, MongoAndInternal}
import org.apache.spark.sql.Row
import org.scalatest.{Matchers, FlatSpec}

class RecordConverterTests extends FlatSpec with Matchers {

  "a flat record with no gaps in the right order" should "convert correctly" in {
    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> 99)
    val t = MongoAndInternal.toInternal(mo)
    val rc = new RecordConverter(t.asInstanceOf[StructureType])
    val r = rc.getSchemaRecord(mo)

    r.size should be (2)
    r.getString(0) should be ("hello")
    r.getInt(1) should be (99)
  }

  "a flat record with no gaps in the wrong order" should "convert correctly" in {
    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> 99)
    val replacement = MongoDBObject("val" -> 99) ++ ("key" -> "hello")

    val t = MongoAndInternal.toInternal(mo)

    val rc = new RecordConverter(t.asInstanceOf[StructureType])
    val r = rc.getSchemaRecord(replacement)

    r should have size (2)
    r.getString(0) should be ("hello")
    r.getInt(1) should be (99)
  }

  "a flat record with gaps in the wrong order" should "convert correctly" in {
    val mo = MongoDBObject("a" -> 1) ++ ("b" -> "2") ++ ("c" -> 3) ++ ("d" -> 4) ++ ("e" -> 5)
    val replacement = MongoDBObject("e" -> 5) ++ ("b" -> "2") ++ ("d" -> 4)

    val t = MongoAndInternal.toInternal(mo)

    val rc = new RecordConverter(t.asInstanceOf[StructureType])
    val r = rc.getSchemaRecord(replacement)

    r should have size (5)
    r.isNullAt(0) should be (true)
    r.getString(1) should be ("2")
    r.isNullAt(2) should be (true)
    r.getInt(3) should be (4)
    r.getInt(4) should be (5)
  }

  "a nested record" should "convert correctly" in {
    val inner1 = MongoDBObject("a" -> 1) ++ ("b" -> "2") ++ ("c" -> 3) ++ ("d" -> 4) ++ ("e" -> 5)
    val inner2 = MongoDBObject("x" -> 11) ++ ("y" -> "12") ++ ("z" -> 13)
    val mo = MongoDBObject("a" -> 1) ++ ("b" -> inner1) ++ ("c" -> 3) ++ ("d" -> inner2) ++ ("e" -> 5)

    val replacement1 = MongoDBObject("a" -> 1) ++ ("c" -> 3) ++ ("d" -> 4) ++ ("e" -> 5)
    val replacement = MongoDBObject("e" -> 5) ++ ("b" -> replacement1) ++ ("d" -> inner2)

    val t = MongoAndInternal.toInternal(mo)

    val rc = new RecordConverter(t.asInstanceOf[StructureType])
    val r = rc.getSchemaRecord(replacement)

    r should have size (5)
    r.isNullAt(0) should be (true)
    r.apply(1) shouldBe a [Row]

    val r1 = r.apply(1).asInstanceOf[Row]
    r1 should have size (5)
    r1.getInt(0) should be (1)
    r1.isNullAt(1) should be (true)
    r1.getInt(2) should be (3)
    r1.getInt(3) should be (4)
    r1.getInt(4) should be (5)

    r.isNullAt(2) should be (true)
    r.apply(3) shouldBe a [Row]

    val r2 = r.apply(3).asInstanceOf[Row]
    r2 should have size (3)
    r2.getInt(0) should be (11)
    r2.getString(1) should be ("12")
    r2.getInt(2) should be (13)

    r.getInt(4) should be (5)

  }

  "a record with an atomic array" should "convert correctly" in {
    val l:BasicDBList = MongoDBList(1,2,3)
    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> l)
    val t = MongoAndInternal.toInternal(mo)
    val rc = new RecordConverter(t.asInstanceOf[StructureType])
    val r = rc.getSchemaRecord(mo)

    r.size should be (2)
    r.getString(0) should be ("hello")
    val s = r.getAs[Seq[Int]](1)
    s.size should be (3)
    s(0) should be (1)
    s(1) should be (2)
    s(2) should be (3)
  }

  "a record with a structured array" should "convert correctly" in {
    val inner1 = MongoDBObject("a" -> 1) ++ ("b" -> 2)
    val inner2 = MongoDBObject("b" -> 3) ++ ("c" -> 4)
    val l:BasicDBList = MongoDBList(inner1, inner2)
    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> l)
    val t = MongoAndInternal.toInternal(mo)
    val rc = new RecordConverter(t.asInstanceOf[StructureType])
    val r = rc.getSchemaRecord(mo)

    r.size should be (2)
    r.getString(0) should be ("hello")
    val s = r.getAs[Seq[Row]](1)
    s.size should be (2)
    s(0) shouldBe a [Row]
    val r1 = s(0).asInstanceOf[Row]
    r1 should have size 3
    r1.getInt(0) should be (1)
    r1.getInt(1) should be (2)
    r1.isNullAt(2) should be (true)
    s(1) shouldBe a [Row]
    val r2 = s(1).asInstanceOf[Row]
    r2 should have size 3
    r2.isNullAt(0) should be (true)
    r2.getInt(1) should be (3)
    r2.getInt(2) should be (4)
  }
}
