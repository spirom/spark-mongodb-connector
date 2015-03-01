package nsmc.rdd

import com.mongodb.BasicDBObject
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

  private val proxy = new CollectionProxy(collectionConfig)

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
    proxy.getPartitions
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    proxy.getPartitionIterator(split, context, new BasicDBObject(), new BasicDBObject()).asInstanceOf[Iterator[R]]
  }

}

