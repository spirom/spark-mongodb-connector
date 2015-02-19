package nsmc.mongo

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient

import com.mongodb.{ServerAddress, DBObject, BasicDBObject}
import org.bson.types.{MaxKey, MinKey}

import scala.collection.mutable

import scala.collection.JavaConversions._

private[nsmc]
class IntervalGenerator(dest: Destination, dbName: String, collectionName: String) extends nsmc.Logging {

  val conf = dest.conf
  val server = new ServerAddress(dest.host, dest.port)
  val client = if (conf.user.isDefined && conf.password.isDefined) {
    val credentials = MongoCredential.createMongoCRCredential(conf.user.get, dbName, conf.password.get.toCharArray)
    logDebug(s"Connecting with password for user '${conf.user.get}'")
    MongoClient(server, List(credentials))
  } else {
    logDebug(s"Connecting without credentials")
    MongoClient(server)
  }

  def close() : Unit = {
    client.close()
  }

  private val MIN_KEY_TYPE = new MinKey()
  private val MAX_KEY_TYPE = new MaxKey()

  private def makeInterval(lowerBound: MongoDBObject, upperBound: MongoDBObject, destination: Destination): MongoInterval = {
    val splitMin:DBObject  = new BasicDBObject()
    if (lowerBound != null) {
      lowerBound.entrySet().iterator().foreach(entry => {
        val key = entry.getKey
        val value = entry.getValue
        if (!value.equals(MIN_KEY_TYPE))
        {
          splitMin.put(key, value)
        }
      })
    }
    val splitMax:DBObject  = new BasicDBObject()
    if (upperBound != null) {
      upperBound.entrySet().iterator().foreach(entry => {
        val key = entry.getKey
        val value = entry.getValue

        if (!value.equals(MAX_KEY_TYPE)) {
          splitMax.put(key, value)
        }
      })
    }
    new MongoInterval(splitMin, splitMax, destination)
  }

  // get the intervals for a sharded collection
  def generate(direct: Boolean = false) : Seq[MongoInterval] = {
    logDebug(s"Generating intervals for sharded collection '$collectionName' in database '$dbName'")

    val intervals = new mutable.ListBuffer[MongoInterval]()

    val coll = client.getDB(dbName)(collectionName)

    val statsOk = coll.getStats.getBoolean("ok", false)

    if (!statsOk) {
      logWarning(s"Can't get stats for collection '$collectionName' in database '$dbName': assuming unsharded")
    }
    if (!statsOk || !coll.getStats.getBoolean("sharded", false)) {
      // all in one partition
      val hostPort = client.getConnectPoint
      val interval = makeInterval(null, null, Destination(hostPort, dest.conf))
    } else {
      // use chunks to create intervals
      val configDb = client.getDB("config")

      // create a map from shards to hosts to know where to connect for chunk data
      val shards = new mutable.HashMap[String, String]()
      val shardsCol = configDb("shards")
      shardsCol.foreach(dbo => shards.+=((dbo.get("_id").asInstanceOf[String], dbo.get("host").asInstanceOf[String])))

      logDebug(s"Shards for '$dbName/$collectionName' count='${shards.size}'")
      shards.foreach(kv => {
        logDebug(s"Shard id='${kv._1}' host:port='${kv._2}'")
      })
      // get the chunks for this collection
      val chunksCol = configDb("chunks")
      chunksCol.foreach(dbo => {
        if (dbo.get("ns").asInstanceOf[String].equals(dbName + "." + collectionName)) {
          val shardName = dbo.get("shard").asInstanceOf[String]

          shards.get(shardName) match {
            case None => {}
            case Some(hostPort) => {
              val interval =
                makeInterval(dbo.get("min").asInstanceOf[DBObject],
                  dbo.get("max").asInstanceOf[DBObject],
                  if (direct) Destination(hostPort, dest.conf) else dest)
              intervals.append(interval)
            }
          }
        }
      })
    }
    intervals
  }

  // Convert a sequence of names of key columns to create a MongoDBObject containing
  // a "keyPattern"
  private def makeKeyPattern(indexedKeys: Seq[(String,Any)]) : MongoDBObject = {
    val builder = MongoDBObject.newBuilder
    // caution: indexes are zero-based but Mongo needs 1-based key sequence
    indexedKeys.foreach({ case (k, v) => builder += (k -> v)})
    builder.result()
  }

  // get intervals for an un-sharded collection as suggested by MongoDB
  def generateSyntheticIntervals(maxChunkSize: Int, indexedKeys: Seq[(String,Any)]) : Seq[MongoInterval] = {
    logDebug(s"Generating synthetic intervals for collection '$collectionName' in database '$dbName' with maxChunkSize='$maxChunkSize'")
    logDebug(s"IndexedKeys: ${indexedKeys.map(k => "[" + k + "]").mkString(", ")}")
    val keyPattern = makeKeyPattern(indexedKeys)
    val splitCommand =
      MongoDBObject("splitVector" -> (dbName + "." + collectionName)) ++
        ("keyPattern" -> keyPattern) ++ ("maxChunkSize" -> maxChunkSize)
    val result = client.getDB(dbName).command(splitCommand)
    val status = result.ok()
    if (!status) {
      throw new MetadataException(result.getErrorMessage)
    }
    val maybeSplitKeys = result.getAs[MongoDBList]("splitKeys")
    val hostPort = client.getConnectPoint

    val destination = Destination(hostPort, dest.conf)

    var previous:MongoDBObject = null
    val intervals = new mutable.ListBuffer[MongoInterval]()
    maybeSplitKeys match {
      case None => {
        logWarning(s"Failed to generate intervals for collection '$collectionName' in database '$dbName'")
      }
      case Some(splitKeys) =>
        splitKeys.foreach(o => {
          val kv = o.asInstanceOf[BasicDBObject]
          val interval = makeInterval(previous, kv, destination)
          logDebug(s"Generated interval ${interval.min},${interval.max} for collection '$collectionName' in database '$dbName'")
          intervals += interval
          previous = kv
        })
      intervals += makeInterval(previous, null, destination)
    }
    logDebug(s"Generated ${intervals.size} intervals for collection '$collectionName' in database '$dbName'")
    intervals
  }
}
