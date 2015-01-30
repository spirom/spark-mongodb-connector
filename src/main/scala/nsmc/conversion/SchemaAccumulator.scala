package nsmc.conversion

import nsmc.conversion.types._

import com.mongodb.casbah.Imports._
import org.apache.spark.sql._

import scala.collection.mutable

class SchemaAccumulator {
  // TODO: make this not be a var by changing the way Merger works
  private var currentInternal = new StructureType(new mutable.HashMap[String, ConversionType]())

  def considerRecord(rec: MongoDBObject) : Unit = {
    val recInternalType = MongoAndInternal.toInternal(rec)
    currentInternal = Merger.merge(recInternalType, currentInternal).asInstanceOf[StructureType]
  }

  def getInternal : StructureType = {
    currentInternal
  }

  def getSchema : Seq[StructField] = {
    InternalAndSchema.toSchema(currentInternal)
  }
}
