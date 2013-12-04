
package org.apache.spark.streaming.dstream

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark._
import scala.Some
import scala.Some
import scala.Some
import org.apache.spark.streaming.Duration

private[streaming]
class WindowedDStream[T: ClassManifest](
                                         parent: DStream[T],
                                         _windowDuration: Duration,
                                         _slideDuration: Duration)
  extends DStream[T](parent.ssc) {

  if (!_windowDuration.isMultipleOf(parent.slideDuration))
    throw new Exception("The window duration of WindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")

  if (!_slideDuration.isMultipleOf(parent.slideDuration))
    throw new Exception("The slide duration of WindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")")

  parent.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration =  _windowDuration

  override def dependencies = List(parent)

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val rddsInWindow = parent.slice(currentWindow)
    val windowRDD = if (rddsInWindow.flatMap(_.partitioner).distinct.length == 1) {
      logInfo("Using partition aware union")
      new PartitionAwareUnionRDD(ssc.sc, rddsInWindow)
    } else {
      logInfo("Using normal union")
      new UnionRDD(ssc.sc,rddsInWindow)
    }
    Some(windowRDD)
  }
}

//object WindowedDStream {
//  import StreamingContext._
//  def main(args: Array[String]) {
//    val ssc = new StreamingContext("local[4]", this.getClass.getSimpleName, Seconds(1))
//    val inputStream = new ConstantInputDStream[Int](ssc, ssc.sparkContext.makeRDD(1 to 100000, 2))
//    inputStream.map(x => (x.toString, x.toString)).reduceByKeyAndWindow(_ + _, Seconds(30)).count.print
//    ssc.start()
//  }
//}

private[streaming]
class PartitionAwareUnionRDDPartition(val idx: Int, val partitions: Array[Partition])
  extends Partition {
  override val index = idx
  override def hashCode(): Int = idx
}

private[streaming]
class PartitionAwareUnionRDD[T: ClassManifest](
                                                sc: SparkContext,
                                                var rdds: Seq[RDD[T]])
  extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.length > 0)
  require(rdds.flatMap(_.partitioner).distinct.length == 1, "Parent RDDs have different partitioners")

  override val partitioner = rdds.head.partitioner

  override def getPartitions: Array[Partition] = {
    val numPartitions = rdds.head.partitions.length
    (0 until numPartitions).map(index => {
      val parentPartitions = rdds.map(_.partitions(index)).toArray
      new PartitionAwareUnionRDDPartition(index, parentPartitions)
    }).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[PartitionAwareUnionRDDPartition].partitions
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }
}





