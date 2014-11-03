import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkContext, SparkConf}

import connector._

class MongoReader {
  val conf = new SparkConf().setAppName("MongoReader").setMaster("local[4]").set("spark.mongo.connection.host", "127.0.0.1").set("spark.mongo.connection.port", "12345")
  val sc = new SparkContext(conf)

  def read() : Unit = {
    // TODO: shouldn't need to use the type parameter below: something may be broken
    val data = sc.mongoCollection[MongoDBObject]("local", "testCollection")
    println("count = " + data.count())
    data.foreach(o => {
      println("<<< " + o + " >>>")
    }
    )
  }
}

object Doit {


  def  main (args: Array[String]) {

    val em = new EmbeddedMongo()

    val mongoClient = MongoClient("localhost", em.getPort())
    mongoClient.dbNames().foreach(println)

    val db = mongoClient.getDB("local")
    val col = db("testCollection")
    col.drop()
    col += MongoDBObject("foo" -> 1) ++ ("name" -> "one")
    col += MongoDBObject("foo" -> 2) ++ ("name" -> "two")

    col.foreach(println)

    // TODO: make sure it's actually in the database

    val mr = new MongoReader()
    mr.read()

    em.stop()
  }
}
