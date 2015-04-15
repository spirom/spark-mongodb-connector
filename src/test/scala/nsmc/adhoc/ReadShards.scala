package nsmc.adhoc

import com.mongodb.casbah.Imports._
import nsmc.mongo.{MongoConnectorConf, Destination, IntervalGenerator, MongoConnector}
import org.apache.spark.SparkConf

object ReadShards {

  def  main (args: Array[String]) {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("spark.nsmc.connection.host", "localhost")
        .set("spark.nsmc.connection.port", "27033")
    val mcc = MongoConnectorConf(conf)
    val ig = new IntervalGenerator(mcc.getDestination(), "shardedtest", "one")
    var tot = 0
    ig.generate().foreach(interval => {
      val iter = new MongoConnector("shardedtest", "one", interval).getData
      var c = 0
      while (iter.hasNext) {
        iter.next()
        c = c + 1
      }
      println(c)
      tot = tot + c
    })
    println("total lines: " + tot)

  }
}