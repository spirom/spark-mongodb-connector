package nsmc.rdd

import com.mongodb.DBObject
import nsmc.Logging
import nsmc.mongo.CollectionProxy
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.language.existentials

// For use only in creating an RDD to return for SQL integration
class SQLMongoRDD private[nsmc] (@transient sc: SparkContext,
                                 proxy: CollectionProxy)
  extends RDD[DBObject](sc, Seq.empty) with Logging {

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

  override def compute(split: Partition, context: TaskContext): Iterator[DBObject] = {
    proxy.getPartitionIterator(split, context)
  }

}

