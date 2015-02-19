package nsmc.mongo


private[nsmc]
case class CollectionConfig(connectorConf: MongoConnectorConf,
                            databaseName: String,
                            collectionName: String,
                            indexedKeys: Seq[(String,Any)]) {

}
