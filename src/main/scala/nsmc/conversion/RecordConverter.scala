package nsmc.conversion

import com.mongodb.casbah.Imports._
import nsmc.conversion.types.{ImmutableStructureType, AtomicType, ConversionType, StructureType}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{IntegerType, StringType}

class RecordConverter(internalType: ImmutableStructureType) extends Serializable {

  def getSchemaRecord(mongoRecord: DBObject) : Row = {
    convert(mongoRecord, internalType)
  }

  private def convert(mongoVal: AnyRef, fieldType: ConversionType) : AnyRef = {
    mongoVal match {
      case o:DBObject => convert(o, fieldType.asInstanceOf[ImmutableStructureType])

      case x => x
    }
  }

  private def convert(mongoRecord: DBObject, internalType: ImmutableStructureType) : Row = {

    // Only include the fields that are found. But since a Row is positional, it's necessary to
    // (a) include a null when the field is not present
    // (b) put them in the order specified by the [internal] schema

    val orderedFields = internalType.sortedFields.map({
      case (name, fieldType) => {
        val mongoValue = mongoRecord.getOrElse(name, null)
        if (mongoValue == null)
          null
        else
          convert(mongoValue, fieldType)
      }
    })

    val schemaRecord = Row(orderedFields:_*)
    schemaRecord
  }

}
