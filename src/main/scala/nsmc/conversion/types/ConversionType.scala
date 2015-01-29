package nsmc.conversion.types

import org.apache.spark.sql.catalyst.types.DataType

import scala.collection.mutable

abstract class ConversionType {
}

case class AtomicType(dt: DataType) extends ConversionType {}

case class StructureType(fields: mutable.HashMap[String, ConversionType]) extends ConversionType {
  def sortedFields = {
    val unsortedFields = fields.toSeq
    unsortedFields.sortBy({ case (name, _) => name })
  }
}
