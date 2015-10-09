package nsmc.conversion

import java.sql
import java.sql.{Timestamp, DatabaseMetaData}
import java.util.Date

import nsmc.conversion.types.{SequenceType, StructureType, ConversionType}
import org.apache.spark.sql.Row
import com.mongodb.casbah.Imports._

class RecordConverter(internalType: StructureType) extends Serializable {

  def getSchemaRecord(mongoRecord: DBObject) : Row = {
    convert(mongoRecord, internalType)
  }

  private def convert(mongoVal: AnyRef, fieldType: ConversionType) : AnyRef = {
    mongoVal match {
      case o:BasicDBList => Seq(o.map(e => convert(e.asInstanceOf[AnyRef], fieldType.asInstanceOf[SequenceType].elementType)):_*)
      case o:DBObject => convert(o, fieldType.asInstanceOf[StructureType])
      case oid:ObjectId => oid.toHexString()
      case d:Date => new Timestamp(d.getTime)
      case x => x
    }
  }

  private def convert(mongoRecord: DBObject, internalType: StructureType) : Row = {

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
