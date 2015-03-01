package nsmc.conversion.types

import scala.collection.immutable.HashMap

object Merger {
  def merge(l: ConversionType, r: ConversionType) : ConversionType = {
    (l, r) match {
      case (StructureType(lMap), StructureType(rMap)) => {
        val keys = lMap.keySet ++ rMap.keySet
        val pairs = keys.map(k => {
          val inLeft = lMap.isDefinedAt(k)
          val inRight = rMap.isDefinedAt(k)
          (inLeft, inRight) match {
            case (true, true) => (k, merge(lMap.getOrElse(k, null), rMap.getOrElse(k, null)))
            case (true, false) => (k, lMap.getOrElse(k, null))
            case (false, true) => (k, rMap.getOrElse(k, null))
            case (false, false) => (k, null) // can't happen
          }
        })
        val ct = new StructureType(HashMap[String, ConversionType](pairs.toSeq:_*))
        ct
      }
      case (_, _) => l // TODO: assume for now they're equal
    }
  }
}
