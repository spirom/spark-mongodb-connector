package nsmc.conversion.types

import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap


class InternalAndSchema {

}

object InternalAndSchema {

  def toSchema(it: ConversionType) : DataType = {
    it match {
      case AtomicType(dt: DataType) => dt
      case SequenceType(et) => ArrayType(toSchema(et))
      case StructureType(fields) => {
        val converted = fields.map(kv => makeField(kv._1, toSchema(kv._2)))
        val sorted = converted.toSeq.sortBy(sf => sf.name)
        StructType(sorted)
      }
    }
  }

  private def makeField(k:String, t: DataType) : StructField = {
    StructField(k, t, nullable = true)
  }

  def toInternal(schema: Seq[StructField]) : ConversionType = {
    val convertedPairs = schema.toSeq.map(toInternal)
    val hm = HashMap[String, ConversionType](convertedPairs:_*)
    new StructureType(hm)
  }

  private def toInternal(sf: StructField) : (String, ConversionType) = {
    sf.dataType match {
      // TODO: leaving out some of the atomic types
      case StringType => (sf.name, AtomicType(StringType))
      case IntegerType => (sf.name, AtomicType(IntegerType))
      case StructType(s) => (sf.name, toInternal(s))
    }
  }

}
