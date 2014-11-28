package nsmc.mongo

import java.net.InetAddress

import org.apache.spark.SparkConf

case class MongoConnectorConf(
  host: String,
  port: Int = MongoConnectorConf.DefaultPort,
  splitIndexed: Boolean,
  splitSize: Int,
  directToShards: Boolean,
  useShardChunks: Boolean
  ) {
  def getDestination() : Destination = Destination(host, port)
}

object MongoConnectorConf {
  val DefaultPort = 27017
  val DefaultSplitSize = 4

  val ConnectionHostProperty = "nsmc.connection.host"
  val ConnectionPortProperty = "nsmc.connection.port"
  val PartitioningSplitIndexedProperty = "nsmc.split.indexed.collections"
  val PartitioningSplitSizeProperty = "nsmc.split.chunk.size"
  val PartitioningDirectToShardsProperty = "nsmc.direct.to.shards"
  val PartitioningUseShardChunksProperty = "nsmc.partition.on.shard.chunks"


  def apply(conf: SparkConf): MongoConnectorConf = {
    val host = conf.get(ConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val port = conf.getInt(ConnectionPortProperty, DefaultPort)
    val splitIndexed = conf.getBoolean(PartitioningSplitIndexedProperty, false)
    val splitSize = conf.getInt(PartitioningSplitSizeProperty, DefaultSplitSize)
    val directToShards = conf.getBoolean(PartitioningDirectToShardsProperty, false)
    val useShardChunks = conf.getBoolean(PartitioningUseShardChunksProperty, false)
    MongoConnectorConf(host, port, splitIndexed, splitSize, directToShards, useShardChunks)
  }
}



