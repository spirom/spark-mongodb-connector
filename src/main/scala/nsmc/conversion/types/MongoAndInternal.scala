package nsmc.conversion.types

import java.util.Date

import com.mongodb.casbah.Imports._
import nsmc.conversion.SchemaAccumulator
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class MongoAndInternal {

}

object MongoAndInternal {
  def toInternal(o: MongoDBObject) : StructureType = {
    val convertedPairs = o.toSeq.map(kv => toInternal(kv))
    val hm = HashMap[String, ConversionType](convertedPairs:_*)
    new StructureType(hm)
  }

  def toInternal(o: BasicDBObject) : StructureType = {
    val convertedPairs = o.toSeq.map(kv => toInternal(kv))
    val hm = HashMap[String, ConversionType](convertedPairs:_*)
    new StructureType(hm)
  }

  def toInternal(a: AnyRef) : ConversionType = {
    a match {
      case d: Date => AtomicType(TimestampType)
      case bts: BSONTimestamp => {
        val s = Seq("inc" -> AtomicType(IntegerType), "time" -> AtomicType(IntegerType))
        StructureType(HashMap[String, ConversionType](s: _*))
      }
      case ba: Array[Byte] => AtomicType(BinaryType)
      case bt: org.bson.types.ObjectId => AtomicType(StringType)
      case _: java.lang.Long => AtomicType(LongType)
      case _: java.lang.Byte => AtomicType(ByteType)
      case _: java.lang.Double => AtomicType(DoubleType)
      case _: java.lang.Boolean => AtomicType(BooleanType)
      case _:String => AtomicType(StringType)
      case _:Integer => AtomicType(IntegerType)
      case o:BasicDBObject => toInternal(o)
      case l:BasicDBList => {
        val sa = new SchemaAccumulator
        l.foreach(dbo => sa.considerDatum(dbo.asInstanceOf[AnyRef]))
        SequenceType(sa.getInternal)
      }
    }
  }

  def toInternal(kv: Pair[String, AnyRef]) : (String, ConversionType) = {
    kv match {
      case (k: String, a: AnyRef) => {
        val vt = toInternal(a)
        (k, vt)
      }
    }
  }
}
