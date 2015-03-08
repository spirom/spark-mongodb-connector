package nsmc.conversion.types

import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

abstract class ConversionType extends Serializable {
}

case class AtomicType(dt: DataType) extends ConversionType with Serializable {}

case class StructureType(fields: HashMap[String, ConversionType]) extends ConversionType {
  def sortedFields = {
    val unsortedFields = fields.toSeq
    unsortedFields.sortBy({ case (name, _) => name })
  }
}

case class SequenceType(elementType: ConversionType) extends ConversionType {

}
