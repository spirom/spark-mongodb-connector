package adhoc

import com.mongodb.casbah.Imports._
import nsmc.mongo.{IntervalGenerator, MongoConnector}

object ReadSimple {

  def  main (args: Array[String]) {

    val mongoClient = MongoClient("localhost", 27030)

    val ig = new IntervalGenerator(mongoClient, "test", "simple2")
    var tot = 0
    ig.generateSyntheticIntervals(4).foreach(interval => {
      val iter = MongoConnector.getCollection("test", "simple", interval)
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