package nsmc.rdd.partitioner

import nsmc.mongo.{MongoInterval, IntervalGenerator, CollectionConfig}
import org.apache.spark.Partition

class MongoRDDPartitioner(val collectionConfig: CollectionConfig) extends nsmc.Logging {
  val ig = new IntervalGenerator(collectionConfig.connectorConf.getDestination(),
    collectionConfig.databaseName, collectionConfig.collectionName)

  def partitions(): Array[Partition] = {
    val intervals = if (collectionConfig.connectorConf.splitIndexed && collectionConfig.indexedKeys.length > 0) {
      logInfo(s"Partitioning collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}' with synthetic partitions")
      ig.generateSyntheticIntervals(collectionConfig.connectorConf.splitSize, collectionConfig.indexedKeys)
    } else if (collectionConfig.connectorConf.useShardChunks) {
      logInfo(s"Partitioning collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}' with shard chunks")
      ig.generate(collectionConfig.connectorConf.directToShards)
    } else {
      logInfo(s"NOT Partitioning collection '${collectionConfig.collectionName}' in database '${collectionConfig.databaseName}' -- producing single partition")
      val interval = new MongoInterval(null, null, collectionConfig.connectorConf.getDestination())
      Seq(interval)
    }
    val partitions = intervals.zipWithIndex map {
      case (interval, index) => {
        val p: Partition = new MongoRDDPartition(index, 0, interval)
        p
      }
    }
    partitions.to[Array]
  }
}
