package nsmc.rdd.partitioner

import nsmc.mongo.{MongoInterval, IntervalGenerator, CollectionConfig}
import org.apache.spark.Partition

class MongoRDDPartitioner(val collectionConfig: CollectionConfig) {
  val ig = new IntervalGenerator(collectionConfig.connectorConf.getDestination(),
    collectionConfig.databaseName, collectionConfig.collectionName)

  def partitions(): Array[Partition] = {
    val intervals = if (collectionConfig.connectorConf.splitIndexed && collectionConfig.indexedKays.length > 0) {
      ig.generateSyntheticIntervals(collectionConfig.connectorConf.splitSize)
    } else if (collectionConfig.connectorConf.useShardChunks) {
      ig.generate(collectionConfig.connectorConf.directToShards)
    } else {
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
