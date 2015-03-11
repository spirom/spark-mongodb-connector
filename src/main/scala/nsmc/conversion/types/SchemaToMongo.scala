package nsmc.conversion.types

import com.mongodb.casbah.Imports._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, IntegerType, StringType, StructField}

object SchemaToMongo {
  def getMongoRecord(schema: Seq[StructField], r: Row) : DBObject = {
    val converted = schema.zip(r.toSeq).map(toMongo)
    MongoDBObject(converted:_*)
  }

  private def toMongo(p:(StructField, Any)) : (String, Any) = {
    p match {
      case (sf, a) =>
        sf.dataType match {
          // TODO: leaving out some of the atomic types
          case StringType => (sf.name, a)
          case IntegerType => (sf.name, a)
          case StructType(s) => (sf.name, getMongoRecord(s, a.asInstanceOf[Row]))
        }
    }
  }


}
