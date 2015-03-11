package nsmc.mongo

// in general the destination's host and port may differ from the configured ones
// because of partitioning and/or replication
private[nsmc]
case class Destination(host: String, port: Int, conf: MongoConnectorConf) {

}

private[nsmc]
case object Destination {
  def apply(hostPort: String, conf: MongoConnectorConf) : Destination = {
    val parts = hostPort.split(":")
    val host = parts(0)
    val port = parts(1).toInt
    new Destination(host, port, conf)
  }

  // Use the configured host and port
  def apply(conf: MongoConnectorConf) : Destination = {
    new Destination(conf.host, conf.port, conf)
  }
}
