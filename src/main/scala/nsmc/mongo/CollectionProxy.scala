package nsmc.mongo

import com.mongodb.DBObject
import nsmc.Logging
import nsmc.rdd.partitioner.{MongoRDDPartition, MongoRDDPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext, Partition}


class CollectionProxy(val collectionConfig: CollectionConfig) extends Logging with Serializable {

  var cachedPartitions: Option[Array[Partition]] = None

  def getPartitions: Array[Partition] = {
    cachedPartitions match {
      case Some(parts) => parts
      case None => {
        val partitioner = new MongoRDDPartitioner(collectionConfig)
        logDebug(s"Created partitioner for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
        try {
          val partitions = partitioner.makePartitions()
          logInfo(s"Obtained ${partitions.size} partitions for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
          cachedPartitions = Some(partitions)
          partitions
        } finally {
          partitioner.close()
          logDebug(s"Closed partitioner for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
        }
      }
    }
  }

  // the caller is ultimately responsible for closing the connector
  def getPartitionConnector(mp: MongoRDDPartition): MongoConnector = {
    logDebug(s"Computing partition ${mp.index} for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    val mongoConnector = new MongoConnector(collectionConfig.databaseName, collectionConfig.collectionName, mp.interval)
    mongoConnector
  }

  def getPartitionIterator(split: Partition, context: TaskContext): Iterator[DBObject] = {
    val mp = split.asInstanceOf[MongoRDDPartition]
    val mongoConnector = getPartitionConnector(mp)
    val iter = mongoConnector.getData
    // TODO: collect statistics here
    context.addTaskCompletionListener { (context) =>
      mongoConnector.close()
      logDebug(s"Computed partition ${mp.index} for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    }
    iter
  }



}
