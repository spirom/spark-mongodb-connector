package nsmc.sql

import com.mongodb.DBObject
import nsmc._
import nsmc.conversion.types.{StructureType}
import nsmc.conversion.{RecordConverter, SchemaAccumulator}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{StructField, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.sources.{TableScan, Filter, PrunedFilteredScan, RelationProvider}

import scala.collection.Iterator
import scala.collection.immutable.HashMap

object PartitionRecordConverter {
  // each partition gets its own converter as there's no reason for the record
  // converters to communicate with each other
  def convert(internalSchema: StructureType)(in: Iterator[DBObject]) : Iterator[Row] = {
    val rc = new RecordConverter(internalSchema)
    in.map(mo => rc.getSchemaRecord(mo))
  }
}

object RowProjector {
  def projectRow(wholeRow: Row,
                         positionalMap: Map[String, Int],
                         requiredColumns: Array[String]): Row = {
    val projected = requiredColumns.map(colName => {
      val pos = positionalMap(colName)
      wholeRow(pos)
    })
    Row(projected:_*)
  }
}

case class MongoTableScan(database: String, collection: String)
                        (@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan with Logging {

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

  def inferType(in: Iterator[DBObject]) : Iterator[StructureType] = {
    val accum = new SchemaAccumulator()
    in.foreach(mo => accum.considerDatum(mo))
    Seq(accum.getInternal.asInstanceOf[StructureType]).iterator
  }

  private def makePositionalMap(fields: Seq[StructField]) : Map[String, Int] = {
    HashMap[String, Int](fields.map(f => f.name).zipWithIndex:_*)
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val schema = internalSchema
    val converter = PartitionRecordConverter.convert(schema.asInstanceOf[StructureType]) _
    val allData = data.mapPartitions(converter, preservesPartitioning = true)
    val positionalMap = makePositionalMap(inferredSchema)
    val projected = allData.map(r => RowProjector.projectRow(r, positionalMap, requiredColumns))
    projected
  }
}

class MongoRelationProvider extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    MongoTableScan(parameters("db"), parameters("collection"))(sqlContext)
  }
}

