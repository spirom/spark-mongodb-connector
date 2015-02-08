package nsmc.conversion

import nsmc.conversion.types.{SequenceType, MongoAndInternal, StructureType, AtomicType}

import com.mongodb.casbah.Imports._
import org.apache.spark.sql.catalyst.types._
import org.scalatest._

class MongoToInternalTests extends FlatSpec with Matchers {

  "a flat object" should "have a flat type" in {

    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> 99)

    val t = MongoAndInternal.toInternal(mo)
    t shouldBe a [StructureType]

    val st = t.asInstanceOf[StructureType]

    st.fields.size should be (2)
    st.fields("key") should be (new AtomicType(StringType))
    st.fields("val") should be (new AtomicType(IntegerType))
  }

  "a nested object" should "have a nested type" in {

    val mo = MongoDBObject("key" -> "hello") ++ ("val" -> (MongoDBObject("a" -> 11) ++ ("b" -> 22)))

    val t = MongoAndInternal.toInternal(mo)

    t shouldBe a [StructureType]
    val st = t.asInstanceOf[StructureType]

    st.fields.size should be (2)
    st.fields("key") should be (new AtomicType(StringType))
    val struct = st.fields("val")
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

  "an array of atomic type" should "have a sequence type" in {
    val l:BasicDBList = MongoDBList(33, 44, 55)
    val mo = MongoDBObject("key" -> "hello") ++ ("vals" -> l)

    val t = MongoAndInternal.toInternal(mo)

    t shouldBe a [StructureType]
    val st = t.asInstanceOf[StructureType]

    st.fields.size should be (2)
    st.fields("key") should be (new AtomicType(StringType))
    st.fields("vals") shouldBe a [SequenceType]
    st.fields("vals") match {
      case SequenceType(elementType) => {
        elementType should be (new AtomicType(IntegerType))
      }
      case _ => fail("can't happen")
    }
  }

  "an array of structure type" should "have a sequence type" in {
    val inner1:BasicDBList = MongoDBList(
      MongoDBObject("a" -> 1),
      MongoDBObject("a" -> 2) ++ ("b" -> "102"),
      MongoDBObject("b" -> "103")
    )
    val mo = MongoDBObject("key" -> "hello") ++ ("vals" -> inner1)

    val t = MongoAndInternal.toInternal(mo)

    t shouldBe a [StructureType]
    val st = t.asInstanceOf[StructureType]

    st.fields.size should be (2)
    st.fields("key") should be (new AtomicType(StringType))
    st.fields("vals") shouldBe a [SequenceType]
    st.fields("vals") match {
      case SequenceType(elementType) => {
        elementType shouldBe a [StructureType]
        elementType match {
          case StructureType(fields) => {
            fields.size should be (2)
            fields("a") should be (new AtomicType(IntegerType))
            fields("b") should be (new AtomicType(StringType))
          }
          case _ => fail("can't happen")
        }
      }
      case _ => fail("can't happen")
    }
  }

  // TODO: remaining atomic types
}
