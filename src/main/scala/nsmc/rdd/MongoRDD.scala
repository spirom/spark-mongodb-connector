package nsmc.rdd

import nsmc.Logging
import nsmc.mongo.{CollectionConfig, MongoConnector}

import nsmc.rdd.partitioner.{MongoRDDPartition, MongoRDDPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}


import scala.language.existentials
import scala.reflect.ClassTag

class MongoRDD[R] private[nsmc] (@transient sc: SparkContext,
                                 val collectionConfig: CollectionConfig)
                                (implicit ct : ClassTag[R])
  extends RDD[R](sc, Seq.empty) with Logging {

  // make sure we inherit logging from the right place: out own Logging class and not RDD
  override def log = super[Logging].log
  override def logName = super[Logging].logName
  override def logInfo(msg: => String) = super[Logging].logInfo(msg)
  override def logDebug(msg: => String) = super[Logging].logDebug(msg)
  override def logTrace(msg: => String) = super[Logging].logTrace(msg)
  override def logWarning(msg: => String) = super[Logging].logWarning(msg)
  override def logError(msg: => String) = super[Logging].logError(msg)
  override def logInfo(msg: => String, throwable: Throwable) = super[Logging].logInfo(msg, throwable)
  override def logDebug(msg: => String, throwable: Throwable) = super[Logging].logDebug(msg, throwable)
  override def logTrace(msg: => String, throwable: Throwable) = super[Logging].logTrace(msg, throwable)
  override def logWarning(msg: => String, throwable: Throwable) = super[Logging].logWarning(msg, throwable)
  override def logError(msg: => String, throwable: Throwable) = super[Logging].logError(msg, throwable)
  override def isTraceEnabled() = super[Logging].isTraceEnabled()

  override def getPartitions: Array[Partition] = {
    val partitioner = new MongoRDDPartitioner(collectionConfig)
    logDebug(s"Created partitioner for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    try {
      val partitions = partitioner.makePartitions()
      logInfo(s"Obtained ${partitions.size} partitions for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
      partitions
    } finally {
      partitioner.close()
      logDebug(s"Closed partitioner for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val mp = split.asInstanceOf[MongoRDDPartition]
    logDebug(s"Computing partition ${mp.index} for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    val mongoConnector = new MongoConnector(collectionConfig.databaseName, collectionConfig.collectionName, mp.interval)
    val iter = mongoConnector.getData.asInstanceOf[Iterator[R]]
    // TODO: collect statistics here
    context.addTaskCompletionListener { (context) =>
      mongoConnector.close()
      logDebug(s"Computed partition ${mp.index} for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    }
    iter
  }

}

