package nsmc.adhoc

import com.mongodb.casbah.Imports._
import nsmc.mongo.{MongoConnectorConf, Destination, IntervalGenerator, MongoConnector}
import org.apache.spark.SparkConf

object ReadSimple {

  def  main (args: Array[String]) {

    val conf =
      new SparkConf()
        .setAppName("MongoReader").setMaster("local[4]")
        .set("nsmc.connection.host", "localhost")
        .set("nsmc.connection.port", "27030")
    val mcc = MongoConnectorConf(conf)
    val ig = new IntervalGenerator(mcc.getDestination(), "test", "simple2")
    var tot = 0
    ig.generateSyntheticIntervals(4, List(("key",1))).foreach(interval => {
      val iter = new MongoConnector("test", "simple", interval).getData
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