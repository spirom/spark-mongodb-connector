import org.apache.spark.SparkContext

/**
 * Created by Spiro on 11/1/2014.
 */
package object nsmc {
  implicit def toContextFunctions(sc: SparkContext): ContextFunctions =
    new ContextFunctions(sc)
}
