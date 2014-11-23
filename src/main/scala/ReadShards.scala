import com.mongodb.casbah.Imports._
import connector.mongo.{MongoConnector, IntervalGenerator}

import scala.collection.mutable



object ReadShards {

  def  main (args: Array[String]) {


    val mongoClient = MongoClient("localhost", 27033)



    val ig = new IntervalGenerator(mongoClient, "shardedtest.one")
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