package connector.rdd.partitioner

import org.apache.spark.Partition

case class MongoRDDPartition(index: Int,
                             rowCount: Long) extends Partition