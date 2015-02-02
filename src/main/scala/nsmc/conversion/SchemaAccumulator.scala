package nsmc.conversion

import nsmc.conversion.types._

import com.mongodb.casbah.Imports._
import org.apache.spark.sql._

import scala.collection.immutable.HashMap

class SchemaAccumulator {
  private var currentInternal = new StructureType(new HashMap[String, ConversionType]())

  def considerRecord(rec: DBObject) : Unit = {
    val recInternalType = MongoAndInternal.toInternal(rec)
    currentInternal = Merger.merge(recInternalType, currentInternal).asInstanceOf[StructureType]
  }

  def accumulate(types: Iterator[StructureType]) : Unit = {
    types.foreach(ty => {
      currentInternal = Merger.merge(ty, currentInternal).asInstanceOf[StructureType]
    })
  }

  def getInternal : StructureType = {
    currentInternal
  }

  def getSchema : Seq[StructField] = {
    InternalAndSchema.toSchema(currentInternal)
  }
}
