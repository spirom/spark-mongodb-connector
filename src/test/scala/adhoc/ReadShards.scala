package adhoc

import com.mongodb.casbah.Imports._
import nsmc.mongo.{Destination, IntervalGenerator, MongoConnector}

object ReadShards {

  def  main (args: Array[String]) {

    val ig = new IntervalGenerator(Destination("localhost", 27033), "shardedtest", "one")
    var tot = 0
    ig.generate().foreach(interval => {
      val iter = MongoConnector.getCollection("shardedtest", "one", interval)
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