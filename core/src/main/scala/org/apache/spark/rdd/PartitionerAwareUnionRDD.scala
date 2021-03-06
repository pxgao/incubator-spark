package org.apache.spark.rdd

import org.apache.spark.{TaskContext, OneToOneDependency, SparkContext, Partition}

/**
 * Created with IntelliJ IDEA.
 * User: peter
 * Date: 12/3/13
 * Time: 5:16 PM
 * To change this template use File | Settings | File Templates.
 */

class PartitionerAwareUnionRDDPartition(val idx: Int, val partitions: Array[Partition])
  extends Partition {
  override val index = idx
  override def hashCode(): Int = idx
}

class PartitionerAwareUnionRDD[T: ClassManifest](
                                                  sc: SparkContext,
                                                  var rdds: Seq[RDD[T]]
                                                  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.length > 0)
  require(rdds.flatMap(_.partitioner).toSet.size == 1,
    "Parent RDDs do not have the same partitioner: " + rdds.flatMap(_.partitioner))

  require(rdds.map(_.partitions.length).toSet.size == 1,
    "Parent RDDs do not have the same num of partitions: " + rdds.map(_.partitions.length) + rdds)


  override val partitioner = rdds.head.partitioner

  override def getPartitions: Array[Partition] = {
    val numPartitions = rdds.head.partitions.length
    (0 until numPartitions).map(index => {
      val parentPartitions = rdds.map(_.partitions(index)).toArray
      new PartitionerAwareUnionRDDPartition(index, parentPartitions)
    }).toArray
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }

  // Get the location where most of the partitions of parent RDDs are located
  override def getPreferredLocations(s: Partition): Seq[String] = {
    logDebug("Getting preferred locations for " + this)
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].partitions
    val locations = rdds.zip(parentPartitions).flatMap {
      case (rdd, part) => {
        val parentLocations = currPrefLocs(rdd, part)
        logDebug("Location of " + rdd + " partition " + part.index + " = " + parentLocations)
        parentLocations
      }
    }

    if (locations.isEmpty) {
      Seq.empty
    } else  {
      Seq(locations.groupBy(x => x).map(x => (x._1, x._2.length)).maxBy(_._2)._1)
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].partitions
//    val iters = rdds.zip(parentPartitions).map {
//      case (rdd, p) => rdd.iterator(p, context)
//    }
//    new CombinedIterator(iters)
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  // gets the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  private def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String] = {
    rdd.context.getPreferredLocs(rdd, part.index).map(tl => tl.host)
  }


  class CombinedIterator[T: ClassManifest](iters : Seq[Iterator[T]]) extends Iterator[T]{
    var remainingSeqs = iters

    var currentIter : Iterator[T]  = Iterator()

    def hasNextRecursive(seqs : Seq[Iterator[T]]) : Boolean = {
      if(seqs.isEmpty)
        false
      else if(seqs.head.hasNext)
        true
      else
        hasNextRecursive(seqs.tail)
    }

    override def hasNext = {
      currentIter.hasNext || hasNextRecursive(remainingSeqs)
    }

    override def next : T = {
      if(currentIter.hasNext)
        currentIter.next()
      else{
        if(remainingSeqs.isEmpty)
          throw new NoSuchElementException("next on empty iterator")
        currentIter = remainingSeqs.head
        remainingSeqs = remainingSeqs.tail
        this.next
      }
    }
  }

}