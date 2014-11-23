package connector.mongo

import com.mongodb.DBObject


case class MongoInterval(min: DBObject, max: DBObject, destination: Destination) {

}
