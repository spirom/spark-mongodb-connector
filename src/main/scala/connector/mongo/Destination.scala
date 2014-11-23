package connector.mongo

case class Destination(host: String, port: Int) {

}

object Destination {
  def apply(hostPort: String) : Destination = {
    val parts = hostPort.split(":")
    val host = parts(0)
    val port = parts(1).toInt
    new Destination(host, port)
  }
}
