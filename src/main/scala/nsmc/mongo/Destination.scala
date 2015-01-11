package nsmc.mongo

private[nsmc]
case class Destination(host: String, port: Int,conf: MongoConnectorConf) {

}

private[nsmc]
object Destination {
  def apply(hostPort: String, conf: MongoConnectorConf) : Destination = {
    val parts = hostPort.split(":")
    val host = parts(0)
    val port = parts(1).toInt
    new Destination(host, port, conf)
  }
}
