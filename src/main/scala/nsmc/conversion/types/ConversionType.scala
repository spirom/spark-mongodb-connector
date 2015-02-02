package nsmc.conversion.types

import org.apache.spark.sql.catalyst.types.DataType

import scala.collection.immutable.HashMap

abstract class ConversionType extends Serializable {
}

case class AtomicType(dt: DataType) extends ConversionType with Serializable {}

case class StructureType(fields: HashMap[String, ConversionType]) extends ConversionType with Serializable {
  def sortedFields = {
    val unsortedFields = fields.toSeq
    unsortedFields.sortBy({ case (name, _) => name })
  }

  def getImmutable: ImmutableStructureType = {
    val convertedFields = fields.toSeq.map({
      case (k, v) => {
        val iv = v match {
          case st: StructureType => st.getImmutable
          case at:AtomicType => at
        }
        (k, iv)
      }
    })
    val ihm = HashMap[String, ConversionType](convertedFields:_*)
    val ist = new ImmutableStructureType(ihm)
    ist
  }
}

case class ImmutableStructureType(fields: HashMap[String, ConversionType]) extends ConversionType with Serializable {
  def sortedFields = {
    val unsortedFields = fields.toSeq
    unsortedFields.sortBy({ case (name, _) => name })
  }
}
