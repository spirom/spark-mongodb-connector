import com.mongodb.casbah.Imports._
import connector.mongo.IntervalGenerator

import scala.collection.mutable



object ReadShards {

  def  main (args: Array[String]) {


    val mongoClient = MongoClient("localhost", 27033)

    val ig = new IntervalGenerator(mongoClient, "shardedtest.one")
    ig.generate().foreach(println)

  }
}