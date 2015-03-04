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
      case EqualTo(attr, v) => Some(attr, v)
      case GreaterThan(attr, v) => Some(attr, MongoDBObject("$gt" -> v))
      case LessThan(attr, v) => Some(attr, MongoDBObject("$lt" -> v))
      case GreaterThanOrEqual(attr, v) => Some(attr, MongoDBObject("$gte" -> v))
      case LessThanOrEqual(attr, v) => Some(attr, MongoDBObject("$lte" -> v))
      case In(attr, vs) => Some(attr, MongoDBObject("$in" -> vs))
      case _ => None
    }
  }

  def makeFilter(pushedFilters: Array[Filter]) : DBObject = {
    val builder = MongoDBObject.newBuilder
    pushedFilters.map(convertFilter).flatten.foreach(p => builder += p)
    builder.result()
  }
}
