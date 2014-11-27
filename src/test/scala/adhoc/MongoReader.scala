package adhoc

import com.mongodb.casbah.Imports._
import org.apache.spark.{SparkConf, SparkContext}

import nsmc._


class MongoReader {
  //val conf = new SparkConf().setAppName("MongoReader").setMaster("local[4]").set("spark.mongo.connection.host", "127.0.0.1").set("spark.mongo.connection.port", "12345")
  val conf = new SparkConf().setAppName("MongoReader").setMaster("local[4]").set("spark.mongo.connection.host", "54.173.12.232").set("spark.mongo.connection.port", "27017")
  val sc = new SparkContext(conf)

  def read() : Unit = {
    // TODO: shouldn't need to use the type parameter below: something may be broken
    // val data = sc.mongoCollection[MongoDBObject]("local", "testCollection")
    val data = sc.mongoCollection[MongoDBObject]("test", "testData")
    println("count = " + data.count())
    data.foreach(o => {
      println("<<< " + o + " >>>")
    }
    )
  }
}
