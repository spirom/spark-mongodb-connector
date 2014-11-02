import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkContext, SparkConf}

import connector._

class MongoReader {
  val conf = new SparkConf().setAppName("MongoReader").setMaster("local[4]")
  val sc = new SparkContext(conf)

  def read() : Unit = {
    val data = sc.mongoCollection("testCollection")
    println("count = " + data.count())
  }
}

object Doit {


  def  main (args: Array[String]) {

    /*
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
    */
    val mr = new MongoReader()
    mr.read()

    //em.stop();
  }
}
