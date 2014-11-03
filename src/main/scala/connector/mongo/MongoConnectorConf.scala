package connector.mongo

import java.net.InetAddress

import org.apache.spark.SparkConf

case class MongoConnectorConf(
  host: String,
  port: Int = MongoConnectorConf.DefaultPort)

object MongoConnectorConf {
  val DefaultPort:Int = 27017

  val MongoConnectionHostProperty = "spark.mongo.connection.host"
  val MongoConnectionPortProperty = "spark.mongo.connection.port"

  def apply(conf: SparkConf): MongoConnectorConf = {
    val host = conf.get(MongoConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val port = conf.getInt(MongoConnectionPortProperty, DefaultPort)
    MongoConnectorConf(host, port)
  }
}



