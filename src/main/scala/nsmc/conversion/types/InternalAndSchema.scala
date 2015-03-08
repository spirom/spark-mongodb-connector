package nsmc.conversion.types

import org.apache.spark.sql.types._


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

}
