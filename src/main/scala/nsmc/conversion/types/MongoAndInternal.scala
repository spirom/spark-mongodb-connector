package nsmc.conversion.types

import com.mongodb.casbah.Imports._
import org.apache.spark.sql.catalyst.types.{IntegerType, StringType}

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

  def toInternal(kv: Pair[String, AnyRef]) : (String, ConversionType) = {
    kv match {
      case (k: String, a: AnyRef) => {
        val vt = a match {
          case bt: org.bson.types.ObjectId => AtomicType(StringType)
          case s:String => AtomicType(StringType)
          case i:Integer => AtomicType(IntegerType)
          case o:BasicDBObject => toInternal(o)
        }
        (k, vt)
      }
    }
  }
}
