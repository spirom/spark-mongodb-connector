package connector.rdd

import connector.mongo.MongoConnector

import java.io.IOException

import connector.rdd.partitioner.MongoRDDPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}


import scala.language.existentials
import scala.reflect.ClassTag



  class MongoRDD[R] private[connector] (
                                             @transient sc: SparkContext,
                                             val connector: MongoConnector
                                         )(
                                             implicit
                                             ct : ClassTag[R])
    extends RDD[R](sc, Seq.empty)  {


    override def getPartitions: Array[Partition] = {
      val partitions = new MongoRDDPartitioner().partitions()
      partitions
    }

    override def getPreferredLocations(split: Partition) =
      Seq()


    override def compute(split: Partition, context: TaskContext): Iterator[R] = {
      Seq[R]().iterator
    }

  }

  object MongoRDD {

  }
