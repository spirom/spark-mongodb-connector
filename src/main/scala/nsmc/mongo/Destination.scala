package nsmc.mongo

case class Destination(host: String, port: Int,conf: MongoConnectorConf) {

}

object Destination {
  def apply(hostPort: String, conf: MongoConnectorConf) : Destination = {
    val parts = hostPort.split(":")
    val host = parts(0)
    val port = parts(1).toInt
    new Destination(host, port, conf)
  }
}
