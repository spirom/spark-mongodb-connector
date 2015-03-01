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
      case GreaterThan(attr, v) => None
      case LessThan(attr, v) => None
      case GreaterThanOrEqual(attr, v) => None
      case LessThanOrEqual(attr, v) => None
      case In(attr, vs) => None
      case _ => None
    }
  }

  def makeFilter(pushedFilters: Array[Filter]) : DBObject = {
    val builder = MongoDBObject.newBuilder
    pushedFilters.map(convertFilter).flatten.foreach(p => builder += p)
    builder.result()
  }
}
