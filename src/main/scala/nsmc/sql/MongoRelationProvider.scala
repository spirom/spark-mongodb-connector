package nsmc.sql

import nsmc._
import nsmc.conversion.{RecordConverter, SchemaAccumulator}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan, RelationProvider}

case class MongoTableScan(database: String, collection: String)
                        (@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan {

  // TODO: think the RDD lifecycle through carefully here

  private val data = sqlContext.sparkContext.mongoCollection(database, collection)
  private val accum = new SchemaAccumulator()
  accum.considerRecord(data.first())
  private val inferredSchema = accum.getSchema

  private val internalSchema = accum.getInternal

  val schema: StructType = StructType(inferredSchema)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val rc = new RecordConverter(internalSchema)
    val convertedData = data.map(mo => rc.getSchemaRecord(mo))
    convertedData
  }
}

class MongoRelationProvider extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    MongoTableScan(parameters("db"), parameters("collection"))(sqlContext)
  }
}

