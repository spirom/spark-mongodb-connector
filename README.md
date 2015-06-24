# NSMC: A Native MongoDB Connector for Apache Spark

This is a native connector for reading and writing MongoDB collections
directly from Apache Spark. In Spark, the data from MongoDB is represented as an
RDD[MongoDBObject]. Starting with Release 0.5.0, you can also use NSMC through Spark SQL
by registering a MongpDB collection as a temporary table. 

Fore more details about this project, check the following blog posts:

[NSMC: A Native MongoDB Connector for Apache Spark](http://www.river-of-bytes.com/2015/01/nsmc-native-mongodb-connector-for.html)

[Spark SQL Integration for MongoDB](http://www.river-of-bytes.com/2015/02/spark-sql-integration-for-mongodb.html)

# Current Limitations

- No Java or Python API bindings
- Can only read (no updates)
- Only tested with the following configurations:
 - MongoDB: 2.6
 - Scala: 2.10
 - Spark: 1.3.0
 - Casbah 2.7
- Not tested with MongoDB's hash-based partitioning

# Other Warnings

- The APIs will probably change several times before an official release

# Release Status

A pre-release alpha version (0.5.2) is available.

# Licensing

See the top-level LICENSE file

# Getting Started

Perhaps the **easiest way to get started** is to use the [companion example project](https://github.com/spirom/spark-mongodb-examples). It contains an appropriately configured **sbt** project, together with sample code for using NSMC from both core Spark and Spark SQL. Be sure to check out that project's README to to make sure you're using the right branch for the NSMC and Apache Spark versions you want to use. 

# **sbt** configuration

Add the following to your build.sbt for the latest _stable_ release:

    scalaVersion := "2.10.4" // any 2.10 is OK -- support for 2.11 coming soon

    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0"

    libraryDependencies += "com.github.spirom" %% "spark-mongodb-connector" % "0.5.2"

# Configuration


| Setting | Meaning | Units | Default |
|---------|---------|-------|---------|
| spark.nsmc.connection.host | MongoDB host or IP address | | localhost |
| spark.nsmc.connection.port | MongoDB port | | 27017 |
| spark.nsmc.user | MongoDB user name | | no authentication |
| spark.nsmc.password | MongoDB password | | no authentication |
| spark.nsmc.split.indexed.collections | Should indexed collections be partitioned using MongoDB's [internal] splitVector command? | boolean | false |
| spark.nsmc.split.chunk.size | Maximum chunk size, in megabytes, passed to MongoDB's splitVector command, if used. | MB | 4 |
| spark.nsmc.partition.on.shard.chunks | Should collections that are already sharded in MongoDB retain this as their partitioning in Spark? If not, the entire collection will be read as a single Spark partition. | boolean | false |
| spark.nsmc.direct.to.shards | If sharding of collections is being observed, should the mongos server be bypassed? (Don't do this unless you understand MongoDB really well, or you may obtain incorrect results -- if MongoDB is rebalancing the shards when your query executes.) | boolean | false |


# Usage from core Spark

In your code, you need to add:

    import nsmc._

    import com.mongodb.DBObject

Then to actually read from a collection:

    val conf = new SparkConf()
        .setAppName("My MongoApp").setMaster("local[4]")
        .set("spark.nsmc.connection.host", "myMongoHost")
        .set("spark.nsmc.connection.port", "myMongoPort")
        .set("spark.nsmc.user", "yourUsernameHere")
        .set("spark.nsmc.password", "yourPasswordHere")
    val sc = new SparkContext(conf)
    val data: MongoRDD[DBObject] =
        sc.mongoCollection[DBObject]("myDB", "myCollection")

# Usage from Spark SQL

Register a temporary table as follows

    sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db 'myDB', collection 'myCollection')
      """.stripMargin)

Query the temporary table as follows

    val data = sqlContext.sql("SELECT * FROM dataTable")

# Summary of releases

| NSMC Release | Status | Apache Spark Release | Scala Version | Features Added |
---------------|--------|----------------------|---------------|----------------|
| 0.5.3 | Alpha | 1.3.0 | 2.10 | Spark 1.4.0 |
| 0.5.2 | Alpha | 1.3.0 | 2.10 | Configuration parameters are prefixed with "spark." to enable use of various Spark tools like the Thrift server and **spark-submit**. |
| 0.5.1 | Alpha | 1.3.0 | 2.10 | Apache Spark dependency is marked as "provided", making it easier to create assemblies that include NSMC. |
| 0.5.0 | Alpha | 1.3.0 | 2.10 | Spark 1.3.0 |
| 0.4.1 | Alpha | 1.2.0 | 2.10 | Filter and Projection push-down for Spark SQL |
| 0.4.0 | Alpha | 1.2.0 | 2.10 | Spark 1.2.0 and Spark SQL Support |
| 0.3.0 | Alpha | 1.1.0 | 2.10 | Initial Release |