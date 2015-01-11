package nsmc.conversion

import org.apache.spark.sql.catalyst.types.DataType

import scala.collection.mutable

abstract class ConversionType {
}

case class AtomicType(dt: DataType) extends ConversionType {}

case class StructureType(fields: mutable.HashMap[String, ConversionType]) extends ConversionType {}
