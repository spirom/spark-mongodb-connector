package nsmc.conversion

import nsmc.conversion.types._

import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class SchemaAccumulator {
  private var currentInternal: Option[ConversionType] = None

  private def maybeMerge(l: ConversionType, ro: Option[ConversionType]) : ConversionType = {
    ro match {
      case Some(r) => Merger.merge(l, r)
      case None => l
    }
  }

  def considerDatum(datum: AnyRef) : Unit = {
    val recInternalType = MongoAndInternal.toInternal(datum)
    currentInternal = Some(maybeMerge(recInternalType, currentInternal))
  }

  def accumulate(types: Iterator[StructureType]) : Unit = {
    types.foreach(ty => {
      currentInternal = Some(maybeMerge(ty, currentInternal))
    })
  }

  def getInternal : ConversionType = {
    currentInternal match {
      case Some(i) => i
      case None => new StructureType(new HashMap[String, ConversionType]())
    }
  }

  // should only be called for a top level (record) schema
  def getSchema : Seq[StructField] = {
    currentInternal match {
      case Some(i) => InternalAndSchema.toSchema(i).asInstanceOf[StructType].fields
      case None => Seq()
    }

  }
}
