import org.apache.spark.SparkContext

import scala.language.implicitConversions

package object nsmc {
  implicit def toContextFunctions(sc: SparkContext): ContextFunctions =
    new ContextFunctions(sc)
}
