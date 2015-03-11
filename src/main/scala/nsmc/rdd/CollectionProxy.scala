package nsmc.rdd

import com.mongodb.{BasicDBObject, DBObject}

import nsmc.Logging
import nsmc.mongo.Destination
import nsmc.conversion.types.SchemaToMongo
import nsmc.mongo.{Destination, MongoInterval, CollectionConfig, MongoConnector}
import nsmc.rdd.partitioner.{MongoRDDPartition, MongoRDDPartitioner}


import org.apache.spark.sql.DataFrame
import org.apache.spark.{Partition, TaskContext}


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

  def getPartitionIterator(split: Partition, context: TaskContext, filter:DBObject, projection:DBObject): Iterator[DBObject] = {
    val mp = split.asInstanceOf[MongoRDDPartition]
    val mongoConnector = getPartitionConnector(mp)
    val iter = mongoConnector.getData(filter, projection)
    // TODO: collect statistics here
    context.addTaskCompletionListener { (context) =>
      mongoConnector.close()
      logDebug(s"Computed partition ${mp.index} for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    }
    iter
  }

  // for now insert into MongoDB sequentially
  def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val interval = MongoInterval(Destination(collectionConfig.connectorConf))
    val mongoConnector = new MongoConnector(collectionConfig.databaseName, collectionConfig.collectionName, interval)
    try {
      val recIter = data.rdd.collect().toIterator.map(r =>
        SchemaToMongo.getMongoRecord(data.schema, r)
      )
      mongoConnector.insert(recIter, overwrite)
    } finally {
      mongoConnector.close()
    }
  }

}
