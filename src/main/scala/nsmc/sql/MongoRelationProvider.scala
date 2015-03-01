package nsmc.sql

import com.mongodb.DBObject
import nsmc._
import nsmc.conversion.types.{StructureType}
import nsmc.conversion.{RecordConverter, SchemaAccumulator}
import nsmc.mongo._
import nsmc.rdd.partitioner.MongoRDDPartition
import nsmc.rdd.{SQLMongoRDD, MongoRDD}

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

class InferenceWrapper(proxy: CollectionProxy) extends Serializable {
  def inferType(part: MongoRDDPartition) : StructureType = {
    val conn = proxy.getPartitionConnector(part)
    val st = inferType(conn)
    conn.close()
    st
  }

  private def inferType(conn: MongoConnector) : StructureType = {
    val in = conn.getData
    val accum = new SchemaAccumulator()
    in.foreach(mo => accum.considerDatum(mo))
    accum.getInternal.asInstanceOf[StructureType]
  }
}

case class MongoTableScan(database: String, collection: String)
                        (@transient val sqlContext: SQLContext)
  extends PrunedFilteredScan with Logging {

  // TODO: make sure we clean up if there's an error

  logInfo(s"Registering MongoDB collection '$collection' in database '$database'")

  // TODO: plumb indexedKeys
  private val collectionConfig = new CollectionConfig(MongoConnectorConf(sqlContext.sparkContext.getConf), database, collection, Seq())

  private val proxy = new CollectionProxy(collectionConfig)

  private val splits = proxy.getPartitions.map(s => s.asInstanceOf[MongoRDDPartition])

  private val partitionRDD = sqlContext.sparkContext.parallelize(splits, splits.size)

  private val inference = new InferenceWrapper(proxy)

  private val partitionSchemas = partitionRDD.map(inference.inferType)

  val partialSchemas = partitionSchemas.collect()

  private val accum = new SchemaAccumulator()
  accum.accumulate(partialSchemas.iterator)

  private val inferredSchema = accum.getSchema

  logDebug(s"Computed schema for collection '$collection' in database '$database'")

  private val internalSchema = accum.getInternal

  val schema: StructType = StructType(inferredSchema)



  private def makePositionalMap(fields: Seq[StructField]) : Map[String, Int] = {
    HashMap[String, Int](fields.map(f => f.name).zipWithIndex:_*)
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // NOTE: it turns out that Spark [at least 1.2.0] sometimes calls this with an empty array of RequiredColumns, for
    // example in order to execute RDD.count() regardless of whether it has yet had a reason to access
    // the actual columns, and thus it produces potentially redundant queries. Presumably it doesn't trust the
    // size of the result set to be invariant with respect the choice of projection columns.
    logDebug(s"Scaning '$database'/'$collection' with columns ${requiredColumns.mkString("[",";","]")}")
    val schema = internalSchema
    val converter = PartitionRecordConverter.convert(schema.asInstanceOf[StructureType]) _
    val queryGenerator = new QueryGenerator()
    val projection = queryGenerator.makeProjection(requiredColumns)

    val queryData = new SQLMongoRDD(sqlContext.sparkContext, proxy, projection)
    val allRows = queryData.mapPartitions(converter, preservesPartitioning = true)
    val positionalMap = makePositionalMap(inferredSchema)
    val projected = allRows.map(r => RowProjector.projectRow(r, positionalMap, requiredColumns))

    projected
  }

}

class MongoRelationProvider extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    MongoTableScan(parameters("db"), parameters("collection"))(sqlContext)
  }
}

