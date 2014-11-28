# A Native MongoDB Connector for Apache Spark

This is a native connector for reading and writing MongoDB collections
directly from Apache Spark. In Spark, the data from MongoDB is represented as an

>    RDD[com.mongodb.casbah.commons.MongoDBObject]

# Current Limitations

- No Java or Python API bindings
- Can only read (no updates)
- Only tested with the following configurations:
 - MongoDB: 2.6
 - Scala: 2.10
 - Spark: 1.1.0
 - Casbah 2.7
- Not tested with MongoDB's hash-based partitioning

# Other Warnings

- The APIs will probably change several times before an official release

# Release Status

Not yet released

# Licensing

See the top-level LICENSE file

# Getting Started