package nsmc.sql

import com.mongodb.DBObject
import com.mongodb.casbah.Imports._
import org.apache.spark.sql.sources._

class QueryGenerator {


  def makeProjection(requiredColumns: Array[String]) : DBObject = {
    val builder = MongoDBObject.newBuilder
    // add all specified columns remembering that _id is in by default
    // and thus has to be explicitly suppressed if not needed
    requiredColumns.foreach(k => if (k != "_id") builder += (k -> 1))
    if (!requiredColumns.contains("_id")) builder += "_id" -> 0
    builder.result()
  }

  private def convertFilter(mongoFilter: Filter) : Option[(String, Any)] = {
    mongoFilter match {
      case EqualTo(attr:String, v:Any) => Some(attr, convertUTF8(v))
      case GreaterThan(attr, v) => Some(attr, MongoDBObject("$gt" -> convertUTF8(v)))
      case LessThan(attr, v) => Some(attr, MongoDBObject("$lt" -> convertUTF8(v)))
      case GreaterThanOrEqual(attr, v) => Some(attr, MongoDBObject("$gte" -> convertUTF8(v)))
      case LessThanOrEqual(attr, v) => Some(attr, MongoDBObject("$lte" -> convertUTF8(v)))
      case In(attr, vs) => Some(attr, MongoDBObject("$in" -> vs.map(convertUTF8(_))))
      case And(f1, f2) => {
        val args = Array(f1, f2)
        val convertedArgs = args.flatMap(convertFilter)
        val objects = convertedArgs.map(p => MongoDBObject(p))
        Some(("$and", objects))
      }
      case Or(f1, f2) => {
        val args = Array(f1, f2)
        val convertedArgs = args.flatMap(convertFilter)
        val objects = convertedArgs.map(p => MongoDBObject(p))
        Some(("$or", objects))
      }
      case IsNull(attr) => Some(attr, null)
      case _ => None
    }
  }

  private def convertUTF8(value: Any) : Any = {
    if (value.isInstanceOf[org.apache.spark.sql.types.UTF8String]) value.toString else value
  }

  def makeFilter(pushedFilters: Array[Filter]) : DBObject = {
    val builder = MongoDBObject.newBuilder
    pushedFilters.flatMap(convertFilter).foreach(p => builder += p)
    builder.result()
  }
}
