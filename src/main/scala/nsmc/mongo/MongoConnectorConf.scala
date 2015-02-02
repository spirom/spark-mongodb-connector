package nsmc.mongo

import java.net.InetAddress

import org.apache.spark.SparkConf

private[nsmc]
case class MongoConnectorConf(
  host: String,
  port: Int = MongoConnectorConf.DefaultPort,
  splitIndexed: Boolean,
  splitSize: Int,
  directToShards: Boolean,
  useShardChunks: Boolean,
  user: Option[String],
  password: Option[String]
  ) {
  def getDestination() : Destination = Destination(host, port, this)
}

private[nsmc]
object MongoConnectorConf extends nsmc.Logging {
  val DefaultPort = 27017
  val DefaultSplitSize = 4

  val ConnectionHostProperty = "nsmc.connection.host"
  val ConnectionPortProperty = "nsmc.connection.port"
  val ConnectionUserProperty = "nsmc.user"
  val ConnectionPasswordProperty = "nsmc.password"
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
    val user = conf.getOption(ConnectionUserProperty)
    val password = conf.getOption(ConnectionPasswordProperty)

    val userString = user.getOrElse("<absent>")
    val passwordString = if (password.isDefined) "<present>" else "<absent>"
    logDebug(s"host='$host' port='$port' user='$userString' password='$passwordString'")

    logDebug(s"$PartitioningSplitIndexedProperty=$splitIndexed")
    logDebug(s"$PartitioningSplitSizeProperty=$splitSize")
    logDebug(s"$PartitioningDirectToShardsProperty=$directToShards")
    logDebug(s"$PartitioningUseShardChunksProperty=$useShardChunks")

    MongoConnectorConf(host, port, splitIndexed, splitSize, directToShards, useShardChunks, user, password)
  }
}



