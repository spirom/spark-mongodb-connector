package nsmc.sql

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import nsmc._
import nsmc.conversion.types.{ImmutableStructureType, StructureType}
import nsmc.conversion.{RecordConverter, SchemaAccumulator}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.sources.{Filter, PrunedFilteredScan, RelationProvider}

import scala.collection.Iterator

object PartitionRecordConverter {
  // each partition gets its own converter as there's no reason for the record
  // converters to communicate with each other
  def convert(internalSchema: ImmutableStructureType)(in: Iterator[DBObject]) : Iterator[Row] = {
    val rc = new RecordConverter(internalSchema)
    in.map(mo => rc.getSchemaRecord(mo))
  }
}

case class MongoTableScan(database: String, collection: String)
                        (@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan with Logging {

  // TODO: think the RDD lifecycle through carefully here

  logInfo(s"Registering MongoDB collection '$collection' in database '$database'")

  private val data = sqlContext.sparkContext.mongoCollection[DBObject](database, collection)
  private val accum = new SchemaAccumulator()

  val sample = data.first()

  accum.considerRecord(sample)
  private val inferredSchema = accum.getSchema

  logDebug(s"Computed schema for collection '$collection' in database '$database'")

  private val internalSchema = accum.getInternal

  val schema: StructType = StructType(inferredSchema)

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val converter = PartitionRecordConverter.convert(internalSchema.getImmutable) _
    data.mapPartitions(converter, preservesPartitioning = true)
  }
}

class MongoRelationProvider extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    MongoTableScan(parameters("db"), parameters("collection"))(sqlContext)
  }
}

