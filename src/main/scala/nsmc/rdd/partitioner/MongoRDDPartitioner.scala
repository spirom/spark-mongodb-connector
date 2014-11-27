package nsmc.rdd.partitioner

import org.apache.spark.Partition

class MongoRDDPartitioner {
  def partitions(): Array[Partition] = {
    Array(MongoRDDPartition(0, 0))
  }
}
