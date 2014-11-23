package connector.mongo

import com.mongodb.casbah.MongoClient

import com.mongodb.{DBObject, BasicDBObject}
import org.bson.types.{MaxKey, MinKey}

import scala.collection.mutable

case class Chunk(id: String, ns:String, shard:String, instance:String)

import scala.collection.JavaConversions._

class IntervalGenerator(client: MongoClient, collectionName: String) {

  private val MIN_KEY_TYPE = new MinKey()
  private val MAX_KEY_TYPE = new MaxKey()

  private def makeInterval(lowerBound: BasicDBObject, upperBound: BasicDBObject, host: String): MongoInterval = {
    val splitMin:DBObject  = new BasicDBObject()
    if (lowerBound != null) {
      lowerBound.entrySet().iterator().foreach(entry => {
        val key = entry.getKey()
        val value = entry.getValue()
        if (!value.equals(MIN_KEY_TYPE))
        {
          splitMin.put(key, value);
        }
      })
    }
    val splitMax:DBObject  = new BasicDBObject()
    if (upperBound != null) {
      upperBound.entrySet().iterator().foreach(entry => {
        val key = entry.getKey();
        val value = entry.getValue();

        if (!value.equals(MAX_KEY_TYPE)) {
          splitMax.put(key, value);
        }
      })
    }
    new MongoInterval(splitMin, splitMax, host)
  }

  def generate() : Seq[MongoInterval] = {
    val configDb = client.getDB("config")

    val shards = new mutable.HashMap[String, String]()
    val shardsCol = configDb("shards")
    shardsCol.foreach(dbo => shards.+=((dbo.get("_id").asInstanceOf[String], dbo.get("host").asInstanceOf[String])))
    shards.foreach(kv => println(kv._1))

    val intervals = new mutable.ListBuffer[MongoInterval]()
    val chunksCol = configDb("chunks")
    chunksCol.foreach(dbo => {
      if (dbo.get("ns").asInstanceOf[String].equals(collectionName)) {
        val shardName = dbo.get("shard").asInstanceOf[String]

        shards.get(shardName) match {
          case None => {}
          case Some(destination) => {
            val interval =
              makeInterval(dbo.get("min").asInstanceOf[BasicDBObject],
                dbo.get("max").asInstanceOf[BasicDBObject],
                destination)
            intervals.append(interval)
          }
        }
      }
    })
    intervals
  }
}
