package nsmc.conversion.types

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.types.StructType


class InternalAndSchema {

}

object InternalAndSchema {
  def toSchema(st: StructureType) : Seq[StructField] = {
    st.fields.map(kv => toSchema(kv)).toSeq.sortBy(sf => sf.name)
  }

  def toSchema(kv: (String, ConversionType)) : StructField = {
    kv match {
      case (k: String, ct: ConversionType) => {
        ct match {
          case AtomicType(dt: DataType) => StructField(k, dt, nullable = true)
          case st: StructureType => StructField(k, StructType(toSchema(st)), nullable = true)
        }
      }
    }
  }
}
