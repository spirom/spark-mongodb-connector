package nsmc.mongo

import com.mongodb.{BasicDBObject, DBObject}

private[nsmc]
case class MongoInterval(min: DBObject, max: DBObject, destination: Destination) extends Serializable {

}

case object MongoInterval {
  def apply(destination: Destination) = new MongoInterval(new BasicDBObject(), new BasicDBObject(), destination)
}
