package nsmc.adhoc

import com.mongodb.casbah.Imports._

object PopulateSimple {

  def  main (args: Array[String]) {


    val mongoClient = MongoClient("localhost", 27030)
    mongoClient.dbNames().foreach(println)

    val db = mongoClient.getDB("test")

    val col = db("simple")
    for (k:Int <- 1 to 300000)
    {
      col += MongoDBObject("key" -> k) ++ ("s" -> ("V" + k))
    }
  }
}