package nsmc.adhoc

import com.mongodb.casbah.Imports._

object PopulateSharded {

  def  main (args: Array[String]) {


    val mongoClient = MongoClient("localhost", 27033)
    mongoClient.dbNames().foreach(println)

    val db = mongoClient.getDB("shardedtest")

    val col = db("one")
    for (k:Int <- 1 to 300000)
    {
      col += MongoDBObject("key" -> k) ++ ("s" -> ("V" + k))
    }
  }
}