package nsmc.rdd.partitioner

import nsmc.mongo.MongoInterval
import org.apache.spark.Partition

private[nsmc]
case class MongoRDDPartition(index: Int,
                             rowCount: Long,
                             interval: MongoInterval)
  extends Partition with Serializable
