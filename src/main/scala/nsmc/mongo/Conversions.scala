package nsmc.mongo

import com.mongodb.casbah.Imports._
import org.apache.spark.sql.StructField
import org.apache.spark.sql.catalyst.types.{StructType, IntegerType, StringType}

class Conversions {

}

object Conversions {
  def toSchema(o: MongoDBObject) : Seq[StructField] = {
    o.map(kv => toSchema(kv)).toSeq
  }

  def toSchema(kv: Pair[String, AnyRef]) : StructField = {
    kv match {
      case (k: String, a: AnyRef) => {
        a match {
          case s:String => StructField(k, StringType, false)
          case i:Integer => StructField(k, IntegerType, false)
          case o:MongoDBObject => StructField(k, StructType(toSchema(o)), false)
        }
      }
    }
  }
}
