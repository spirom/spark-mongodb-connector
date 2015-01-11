package nsmc.mongo

import com.mongodb.DBObject

private[nsmc]
case class MongoInterval(min: DBObject, max: DBObject, destination: Destination) {

}
