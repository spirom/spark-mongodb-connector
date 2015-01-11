package nsmc.adhoc

import util.EmbeddedMongo

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient

object ConnectionCheck {
  def  main (args: Array[String]) {
    val em = new EmbeddedMongo()


    val mongoClient = MongoClient("127.0.0.1", em.getPort())
    mongoClient.dbNames().foreach(println)

    val db = mongoClient.getDB("local")
    val col = db("testCollection")
    col.drop()
    col += MongoDBObject("foo" -> 1) ++ ("name" -> "one")
    col += MongoDBObject("foo" -> 2) ++ ("name" -> "two")

    col.foreach(println)

    em.stop();
  }
}
