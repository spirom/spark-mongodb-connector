package nsmc.sql

import com.mongodb.DBObject
import nsmc._
import nsmc.conversion.types.{StructureType}
import nsmc.conversion.{RecordConverter, SchemaAccumulator}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.sources.{TableScan, Filter, PrunedFilteredScan, RelationProvider}

import scala.collection.Iterator

object PartitionRecordConverter {
  // each partition gets its own converter as there's no reason for the record
  // converters to communicate with each other
  def convert(internalSchema: StructureType)(in: Iterator[DBObject]) : Iterator[Row] = {
    val rc = new RecordConverter(internalSchema)
    in.map(mo => rc.getSchemaRecord(mo))
  }
}

case class MongoTableScan(database: String, collection: String)
                        (@transient val sqlContext: SQLContext)
  extends TableScan with Logging {

  // TODO: think the RDD lifecycle through carefully here

  logInfo(s"Registering MongoDB collection '$collection' in database '$database'")

  private val data = sqlContext.sparkContext.mongoCollection[DBObject](database, collection)

  private val partitionSchemas = data.mapPartitions(inferType, preservesPartitioning = true)

  val partialSchemas = partitionSchemas.collect()

  private val accum = new SchemaAccumulator()
  accum.accumulate(partialSchemas.iterator)

  private val inferredSchema = accum.getSchema

  logDebug(s"Computed schema for collection '$collection' in database '$database'")

  private val internalSchema = accum.getInternal

  val schema: StructType = StructType(inferredSchema)
  schema.printTreeString()

  def inferType(in: Iterator[DBObject]) : Iterator[StructureType] = {
    val accum = new SchemaAccumulator()
    in.foreach(mo => accum.considerRecord(mo))
    Seq(accum.getInternal).iterator
  }

  def buildScan: RDD[Row] = {
    val schema = internalSchema
    val converter = PartitionRecordConverter.convert(schema) _
    data.mapPartitions(converter, preservesPartitioning = true)
  }
}

class MongoRelationProvider extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    MongoTableScan(parameters("db"), parameters("collection"))(sqlContext)
  }
}

