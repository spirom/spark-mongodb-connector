package adhoc

import com.mongodb.casbah.Imports._
import nsmc.mongo.{Destination, IntervalGenerator, MongoConnector}

object ReadSimple {

  def  main (args: Array[String]) {

    val ig = new IntervalGenerator(Destination("localhost", 27030), "test", "simple2")
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