package nsmc.rdd

import nsmc.mongo.{CollectionConfig, MongoConnector}

import nsmc.rdd.partitioner.{MongoRDDPartition, MongoRDDPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}


import scala.language.existentials
import scala.reflect.ClassTag

class MongoRDD[R] private[nsmc] (@transient sc: SparkContext,
                                 val collectionConfig: CollectionConfig)
                                (implicit ct : ClassTag[R])
  extends RDD[R](sc, Seq.empty)  {

  override def getPartitions: Array[Partition] = {
    val partitioner = new MongoRDDPartitioner(collectionConfig)
    val partitions = partitioner.makePartitions()
    partitioner.close()
    // TODO: is this using the wrong logger?
    logInfo(s"Obtained ${partitions.size} partitions for collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}'")
    partitions
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

