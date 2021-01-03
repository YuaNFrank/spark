/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage.memory

import java.io.OutputStream
import java.nio.ByteBuffer
import java.util.{LinkedHashMap, UUID}
import scala.collection.JavaConverters._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.{Set, ListMap}
import scala.reflect.ClassTag
import com.google.common.io.ByteStreams
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.serializer.{SerializationStream, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockInfoManager, BlockManager, RDDBlockId, StorageLevel, TempLocalBlockId, TestBlockId}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import scala.collection.immutable.ListMap
import scala.util.control.Breaks.{break, breakable}

private sealed trait MemoryEntry[T] {
  def size: Long
  def memoryMode: MemoryMode
  def classTag: ClassTag[T]
}
private case class DeserializedMemoryEntry[T](
    value: Array[T],
    size: Long,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  val memoryMode: MemoryMode = MemoryMode.ON_HEAP
}
private case class SerializedMemoryEntry[T](
    buffer: ChunkedByteBuffer,
    memoryMode: MemoryMode,
    classTag: ClassTag[T]) extends MemoryEntry[T] {
  def size: Long = buffer.size
}

private[storage] trait BlockEvictionHandler {
  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * The caller of this method must hold a write lock on the block before calling this method.
   * This method does not release the write lock.
   *
   * @return the block's new effective StorageLevel.
   */
  private[storage] def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]): StorageLevel
}

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 */
private[spark] class MemoryStore(
    conf: SparkConf,
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    memoryManager: MemoryManager,
    blockEvictionHandler: BlockEvictionHandler)
  extends Logging {

  // Note: all changes to memory allocations, notably putting blocks, evicting blocks, and
  // acquiring or releasing unroll memory, ```must be synchronized on `memoryManager`!

  // leasing:
  var DAGInfoMap = mutable.HashMap[Int, mutable.HashMap[Int, Int]]() // we use DAGInfoMap only for calculating leasing
  var currentDAGInfoMap = mutable.HashMap[Int, mutable.HashMap[Int, Int]]()
  var globalDAG = mutable.HashMap[Int, mutable.HashMap[Int, Int]] ()
  var AccessNumberGlobal = 0

  // total number of data block that woulb be reused
  private def totalNumberOfDataBlocks = DAGInfoMap.count(kv => kv._2.nonEmpty)

  private val leaseMap = mutable.HashMap[Int, Int]()
  private val currentLease = mutable.HashMap[Int, Int]()


  // Require M -- Total number of data blocks =>  total Number of RDD for this job => DAGInfoMap.size
  // Require R -- Max. distinct reuse intervals per block => DAGInfoMap(rddId).size - 1
    // IMPORTANT!!:: Here we need to reduce the number by 1 since it is the worst case
  // Require RI[1...M][1...R]  -- Reuse Interval histogram.  => DAGInfoMap

  def HITS(rddid: Int, maxRi: Int): Double = {

    var hit = 0
    for ( (ri , freq) <- DAGInfoMap.getOrElse(rddid, mutable.HashMap[Int, Int]()) ) {
      if (ri <= maxRi) {
        hit += freq
      }
    }
    hit
  }

  def COST(rddid: Int, lease: Int): Double = {

    var upper = 0
    var lower = 0
    for ( (ri, freq) <- DAGInfoMap.getOrElse(rddid, mutable.HashMap[Int, Int]()) ) {
      if ( ri <= lease ) {
        upper += ri * freq
      } else {
        lower += lease * freq
      }
    }
    upper + lower
  }

   def getPPUC(rddid: Int, oldLease: Int, newLease: Int): Double = {
    if (COST(rddid, newLease) - COST(rddid, oldLease) != 0) {
       (HITS(rddid, newLease) - HITS(rddid, oldLease)) / (COST(rddid, newLease) - COST(rddid, oldLease))
    } else {
       0.0
    }
  }

  /*
  * @Require: M -- Total number of data blocks -- totalNumberOfDataBlocks
  * @Require: N -- Total number of access -- AccessNumberGlobal
  * @Require: R -- Max. distinct reuse intervals per block => DAGInfoMap(rddId).size - 1
  * @Require: RI -- reuse interval histograms => DAGInfoMap
  * @Require: C -- Target cache size  => maximum cache size => maxMemory
  * @Ensure: L -- Assigned leases => leaseMap
  * */
  def OSL():Unit = {

    def maxPPUC(): (Boolean, Int, Int) = {
      var bestPPUC = 0.0
      var bestBlock = (true, 0, 0)
      var reuse = 0
      var ppuc = 0.0
      for (block <- DAGInfoMap.keySet) {
        for ( (ri, freq) <- DAGInfoMap(block) ) {
          if ( ri > leaseMap.getOrElseUpdate(block, 0)) {
            reuse = ri
            ppuc = getPPUC(block, leaseMap(block), reuse)
            if (ppuc > bestPPUC) {
              bestPPUC = ppuc
              bestBlock = (false, block, reuse)
            }
          }
        }
      }
       bestBlock
    }

    for ((block, _) <- DAGInfoMap) {
      leaseMap(block) = 0
    }

    var totalCost : Double = 0
    val targetCost =  getAverageCacheSize * AccessNumberGlobal // here we do not apply the target cache size, instead we use the total case size
    // to constrain the lease

    while (totalCost <= targetCost) {
      val (finished, block, newLease) = maxPPUC()
      if (!finished) {
        val oldLease = leaseMap(block)
        val cost = COST(block, newLease) - COST(block, oldLease)
        totalCost = totalCost + cost
        leaseMap(block) = newLease
      } else {
        println("Leasing Assign finished")
        return
      }
    }
  }

  // deduct the lease by one when access the cache
  def deductLease(blockId: BlockId) : Unit = {
    logWarning(s"Leasing: Drop all lease by 1")
    currentLease.synchronized {
      for ( (k,v) <- currentLease){
        currentLease(k)  = if (v -1 >0) v - 1 else 0
      }
    }
    val rddid = blockId.asRDDId.toString.split("_")(1).toInt
    // lease fitting
    currentLease.synchronized {
      if (currentLease.contains(rddid)) {
        currentLease(rddid) = leaseMap.getOrElse(rddid, 0)
      } else {
        currentLease.put(rddid, leaseMap.getOrElse(rddid, 0))
      }
    }
  }



  // how many memory block can be cached over the last iteration
  private def getAverageCacheSize: Long = {
    var totalMem : Long = 0
     if (entries.asScala.keySet.count(x => x.isRDD) > 0) {
//       entries.size()
       entries.asScala.keySet.count(x => x.isRDD)
     } else {
       DAGInfoMap.size
     }
  }

  private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)
  var refMap = mutable.HashMap[BlockId, Int]()  // yyh no recency. remaining refCount of
  // all blocks in cache and disk
  var currentRefMap = mutable.HashMap[BlockId, Int]() // remaining refCount of blocks in cache


  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `memoryManager`
  private val onHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()
  // Note: off-heap unroll memory is only used in putIteratorAsBytes() because off-heap caching
  // always stores serialized values.
  private val offHeapUnrollMemoryMap = mutable.HashMap[Long, Long]()

  // Initial memory to request before unrolling any block
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)

  /** Total amount of memory available for storage, in bytes. */
  private def maxMemory: Long = memoryManager.maxOnHeapStorageMemory

  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Total storage memory used including unroll memory, in bytes. */
  private def memoryUsed: Long = memoryManager.storageMemoryUsed

  /**
   * Amount of storage memory, in bytes, used for caching blocks.
   * This does not include memory used for unrolling.
   */
  private def blocksMemoryUsed: Long = memoryManager.synchronized {
    memoryUsed - currentUnrollMemory
  }

  def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   *
   * The caller should guarantee that `size` is correct.
   *
   * @return true if the put() succeeded, false otherwise.
   */
  def putBytes[T: ClassTag](
      blockId: BlockId,
      size: Long,
      memoryMode: MemoryMode,
      _bytes: () => ChunkedByteBuffer): Boolean = {
    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")
    if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {

      if (blockId.isRDD) { // we only care about the ref count of rdd blocks
        val rddId = blockId.asRDDId.toString.split("_")(1).toInt // yyh asRDDId: rdd_1_1
        if (refMap.contains(blockId)){
          logError(s"LRC: the to unrolled block is already in the ref map")
        }
        else if (blockManager.refProfile_online.contains(rddId)) {   // jobDAG
          val ref_count = blockManager.refProfile_online(rddId)// jobDAG
          refMap.synchronized { refMap(blockId) = ref_count}
          logInfo(s"LRC: (Unrolling) fetch the ref count of $blockId: $ref_count")
        }
        else {
          refMap.synchronized {refMap.put(blockId, 1)}
          logError(s"LRC: the unrolled block $blockId is not in the ref profile")
        }
      }

      if (blockId.isRDD) {
        val rddId = blockId.asRDDId.toString.split("_")(1).toInt
        if (DAGInfoMap.contains(rddId)) {
          logWarning("Leasing: The to unrooled block, we got its RI")
          currentDAGInfoMap.put(rddId, DAGInfoMap(rddId))
          currentLease.put(rddId, leaseMap.getOrElse(rddId, 0))
        } else if (globalDAG.contains(rddId)) {
          logWarning("Leasing: The to unrooled block, we got its RI from the global dag")
          currentDAGInfoMap.put(rddId, globalDAG(rddId))
          currentLease.put(rddId, leaseMap.getOrElse(rddId, 0))
        }
      }

      // We acquired enough memory for the block, so go ahead and put it
      val bytes = _bytes()
      assert(bytes.size == size)
      val entry = new SerializedMemoryEntry[T](bytes, memoryMode, implicitly[ClassTag[T]])
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      true
    } else {
      false
    }
  }

  /**
   * Attempt to put the given block in memory store as values.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   *
   * @return in case of success, the estimated the estimated size of the stored data. In case of
   *         failure, return an iterator containing the values of the block. The returned iterator
   *         will be backed by the combination of the partially-unrolled block and the remaining
   *         elements of the original input iterator. The caller must either fully consume this
   *         iterator or call `close()` on it in order to free the storage memory consumed by the
   *         partially-unrolled block.
   */
  private[storage] def putIteratorAsValues[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T]): Either[PartiallyUnrolledIterator[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    // Number of elements unrolled so far
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    val memoryGrowthFactor = 1.5
    // Keep track of unroll memory used by this particular block / putIterator() operation
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying vector for unrolling the block
    var vector = new SizeTrackingVector[T]()(classTag)

    // yyh: Get the ref count of the to-unroll block, no matter whether
    // it is finally unrolled successfully or not
    if (blockId.isRDD) { // we only care about the ref count of rdd blocks
      val rddId = blockId.asRDDId.toString.split("_")(1).toInt // yyh asRDDId: rdd_1_1
      if (refMap.contains(blockId)){
        logError(s"LRC: the to unrolled block is already in the ref map")
      }
      else if (blockManager.refProfile_online.contains(rddId)) {   // jobDAG
        val ref_count = blockManager.refProfile_online(rddId)// jobDAG
        refMap.synchronized { refMap(blockId) = ref_count}
        logInfo(s"LRC: (Unrolling) fetch the ref count of $blockId: $ref_count")
      }
      else {
        refMap.synchronized {refMap.put(blockId, 1)}
        logError(s"LRC: the unrolled block $blockId is not in the ref profile")
      }
    }




    // Request enough memory to begin unrolling
    keepUnrolling =
      reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, MemoryMode.ON_HEAP)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    while (values.hasNext && keepUnrolling) {
      vector += values.next()
      if (elementsUnrolled % memoryCheckPeriod == 0) {
        // If our vector's size has exceeded the threshold, request more memory
        val currentSize = vector.estimateSize()
        if (currentSize >= memoryThreshold) {
          val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
          keepUnrolling =
            reserveUnrollMemoryForThisTask(blockId, amountToRequest, MemoryMode.ON_HEAP)
          if (keepUnrolling) {
            unrollMemoryUsedByThisBlock += amountToRequest
          }
          // New threshold is currentSize * memoryGrowthFactor
          memoryThreshold += amountToRequest
        }
      }
      elementsUnrolled += 1
    }

    if (keepUnrolling) {
      // We successfully unrolled the entirety of this block
      val arrayValues = vector.toArray
      vector = null
      val entry =
        new DeserializedMemoryEntry[T](arrayValues, SizeEstimator.estimate(arrayValues), classTag)
      val size = entry.size
      def transferUnrollToStorage(amount: Long): Unit = {
        // Synchronize so that transfer is atomic
        memoryManager.synchronized {
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, amount)
          val success = memoryManager.acquireStorageMemory(blockId, amount, MemoryMode.ON_HEAP)
          assert(success, "transferring unroll memory to storage memory failed")
        }
      }
      // Acquire storage memory if necessary to store this block in memory.
      val enoughStorageMemory = {
        if (unrollMemoryUsedByThisBlock <= size) {
          val acquiredExtra =
            memoryManager.acquireStorageMemory(
              blockId, size - unrollMemoryUsedByThisBlock, MemoryMode.ON_HEAP)
          if (acquiredExtra) {
            transferUnrollToStorage(unrollMemoryUsedByThisBlock)
          }
          acquiredExtra
        } else { // unrollMemoryUsedByThisBlock > size
          // If this task attempt already owns more unroll memory than is necessary to store the
          // block, then release the extra memory that will not be used.
          val excessUnrollMemory = unrollMemoryUsedByThisBlock - size
          releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP, excessUnrollMemory)
          transferUnrollToStorage(size)
          true
        }
      }

      // yyh: Get the ref count of the to-cache block, no matter whether it is finally cached or not
      if (blockId.isRDD) { // we only care about the ref count of rdd blocks
        val rddId = blockId.asRDDId.toString.split("_")(1).toInt // yyh asRDDId: rdd_1_1
        if (refMap.contains(blockId)){
          logInfo(s"LRC: the to be cache block is already in the ref map")
        }
        else if (blockManager.refProfile_online.contains(rddId)) {// jobDAG
          val ref_count = blockManager.refProfile_online(rddId)// jobDAG
          refMap.synchronized { refMap(blockId) = ref_count}
          logInfo(s"LRC: fetch the ref count of $blockId: $ref_count")
        }
        else {
          refMap.synchronized {refMap.put(blockId, 1)}
          logInfo(s"LRC: block $blockId is not in the ref profile")
        }
      }


//      if (blockId.isRDD) {
//        val rddId = blockId.asRDDId.toString.split("_")(1).toInt
//        if (DAGInfoMap.contains(rddId)) {
//          logWarning(s"Leasing: the to be cached block is already in the DAG map")
//        } else if (blockManager.DAGProfile_Online.contains(rddId)) {
//          val ri = blockManager.DAGProfile_Online(rddId)
//          DAGInfoMap.synchronized {DAGInfoMap(rddId) = ri}
//        } else {
//          DAGInfoMap.synchronized {DAGInfoMap.put(rddId, new mutable.HashMap[Int, Int]())}
//          logError(s"Leasing: the unrolled block $blockId from $rddId is not in the DAGmap")
//        }
//      }


      if (enoughStorageMemory) {
        entries.synchronized {
          entries.put(blockId, entry)
        }
        if (blockId.isRDD) {
          currentRefMap.synchronized{
            currentRefMap.put(blockId, refMap(blockId))
            val ref_count = refMap(blockId)
            logInfo(s"LRC: put $blockId in current ref map: $ref_count")
          }
        }
       // LEasing: assign lease to the in-coming block
        if (blockId.isRDD) {
          val rddId = blockId.asRDDId.toString.split("_")(1).toInt
          if (DAGInfoMap.contains(rddId)) {
            logWarning("Leasing: The to unrooled block, we got its RI")
            currentDAGInfoMap.put(rddId, DAGInfoMap(rddId))
            currentLease.put(rddId, leaseMap.getOrElse(rddId, 1))
          } else if (globalDAG.contains(rddId)) {
            logWarning("Leasing: The to unrooled block, we got its RI from the global dag")
            currentDAGInfoMap.put(rddId, globalDAG(rddId))
            currentLease.put(rddId, leaseMap.getOrElse(rddId, 1))
          }
        }

        logInfo("Block %s stored as values in memory (estimated size %s, free %s)".format(
          blockId, Utils.bytesToString(size), Utils.bytesToString(maxMemory - blocksMemoryUsed)))
        Right(size)
      } else {
        assert(currentUnrollMemoryForThisTask >= unrollMemoryUsedByThisBlock,
          "released too much unroll memory")
        Left(new PartiallyUnrolledIterator(
          this,
          MemoryMode.ON_HEAP,
          unrollMemoryUsedByThisBlock,
          unrolled = arrayValues.toIterator,
          rest = Iterator.empty))
      }
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, vector.estimateSize())
      Left(new PartiallyUnrolledIterator(
        this,
        MemoryMode.ON_HEAP,
        unrollMemoryUsedByThisBlock,
        unrolled = vector.iterator,
        rest = values))
    }
  }

  /**
   * Attempt to put the given block in memory store as bytes.
   *
   * It's possible that the iterator is too large to materialize and store in memory. To avoid
   * OOM exceptions, this method will gradually unroll the iterator while periodically checking
   * whether there is enough free memory. If the block is successfully materialized, then the
   * temporary unroll memory used during the materialization is "transferred" to storage memory,
   * so we won't acquire more memory than is actually needed to store the block.
   *
   * @return in case of success, the estimated the estimated size of the stored data. In case of
   *         failure, return a handle which allows the caller to either finish the serialization
   *         by spilling to disk or to deserialize the partially-serialized block and reconstruct
   *         the original input iterator. The caller must either fully consume this result
   *         iterator or call `discard()` on it in order to free the storage memory consumed by the
   *         partially-unrolled block.
   */
  private[storage] def putIteratorAsBytes[T](
      blockId: BlockId,
      values: Iterator[T],
      classTag: ClassTag[T],
      memoryMode: MemoryMode): Either[PartiallySerializedBlock[T], Long] = {

    require(!contains(blockId), s"Block $blockId is already present in the MemoryStore")

    val allocator = memoryMode match {
      case MemoryMode.ON_HEAP => ByteBuffer.allocate _
      case MemoryMode.OFF_HEAP => Platform.allocateDirectBuffer _
    }

    // Whether there is still enough memory for us to continue unrolling this block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes).
    val initialMemoryThreshold = unrollMemoryThreshold
    // Keep track of unroll memory used by this particular block / putIterator() operation
    var unrollMemoryUsedByThisBlock = 0L
    // Underlying buffer for unrolling the block
    val redirectableStream = new RedirectableOutputStream
    val bbos = new ChunkedByteBufferOutputStream(initialMemoryThreshold.toInt, allocator)
    redirectableStream.setOutputStream(bbos)
    val serializationStream: SerializationStream = {
      val ser = serializerManager.getSerializer(classTag).newInstance()
      ser.serializeStream(serializerManager.wrapForCompression(blockId, redirectableStream))
    }

    // Request enough memory to begin unrolling
    keepUnrolling = reserveUnrollMemoryForThisTask(blockId, initialMemoryThreshold, memoryMode)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    } else {
      unrollMemoryUsedByThisBlock += initialMemoryThreshold
    }

    def reserveAdditionalMemoryIfNecessary(): Unit = {
      if (bbos.size > unrollMemoryUsedByThisBlock) {
        val amountToRequest = bbos.size - unrollMemoryUsedByThisBlock
        keepUnrolling = reserveUnrollMemoryForThisTask(blockId, amountToRequest, memoryMode)
        if (keepUnrolling) {
          unrollMemoryUsedByThisBlock += amountToRequest
        }
      }
    }

    // Unroll this block safely, checking whether we have exceeded our threshold
    while (values.hasNext && keepUnrolling) {
      serializationStream.writeObject(values.next())(classTag)
      reserveAdditionalMemoryIfNecessary()
    }

    // Make sure that we have enough memory to store the block. By this point, it is possible that
    // the block's actual memory usage has exceeded the unroll memory by a small amount, so we
    // perform one final call to attempt to allocate additional memory if necessary.
    if (keepUnrolling) {
      serializationStream.close()
      reserveAdditionalMemoryIfNecessary()
    }

    if (keepUnrolling) {

      if (blockId.isRDD) { // we only care about the ref count of rdd blocks
        val rddId = blockId.asRDDId.toString.split("_")(1).toInt // yyh asRDDId: rdd_1_1
        if (refMap.contains(blockId)){
          logError(s"LRC: the to unrolled block is already in the ref map")
        }
        else if (blockManager.refProfile_online.contains(rddId)) {   // jobDAG
          val ref_count = blockManager.refProfile_online(rddId)// jobDAG
          refMap.synchronized { refMap(blockId) = ref_count}
          logInfo(s"LRC: (Unrolling) fetch the ref count of $blockId: $ref_count")
        }
        else {
          refMap.synchronized {refMap.put(blockId, 1)}
          logError(s"LRC: the unrolled block $blockId is not in the ref profile")
        }
      }

      if (blockId.isRDD) {
        val rddId = blockId.asRDDId.toString.split("_")(1).toInt
        if (DAGInfoMap.contains(rddId)) {
          logWarning("Leasing: The to unrooled block, we got its RI")
          currentDAGInfoMap.put(rddId, DAGInfoMap(rddId))
          currentLease.put(rddId, leaseMap.getOrElse(rddId, 1))
        } else if (globalDAG.contains(rddId)) {
          logWarning("Leasing: The to unrooled block, we got its RI from the global dag")
          currentDAGInfoMap.put(rddId, globalDAG(rddId))
          currentLease.put(rddId, leaseMap.getOrElse(rddId, 1))
        }
      }

      val entry = SerializedMemoryEntry[T](bbos.toChunkedByteBuffer, memoryMode, classTag)
      // Synchronize so that transfer is atomic
      memoryManager.synchronized {
        releaseUnrollMemoryForThisTask(memoryMode, unrollMemoryUsedByThisBlock)
        val success = memoryManager.acquireStorageMemory(blockId, entry.size, memoryMode)
        assert(success, "transferring unroll memory to storage memory failed")
      }
      entries.synchronized {
        entries.put(blockId, entry)
      }
      logInfo("Block %s stored as bytes in memory (estimated size %s, free %s)".format(
        blockId, Utils.bytesToString(entry.size),
        Utils.bytesToString(maxMemory - blocksMemoryUsed)))
      Right(entry.size)
    } else {
      // We ran out of space while unrolling the values for this block
      logUnrollFailureMessage(blockId, bbos.size)
      Left(
        new PartiallySerializedBlock(
          this,
          serializerManager,
          blockId,
          serializationStream,
          redirectableStream,
          unrollMemoryUsedByThisBlock,
          memoryMode,
          bbos,
          values,
          classTag))
    }
  }

  def getBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: DeserializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getBytes on serialized blocks")
      case SerializedMemoryEntry(bytes, _, _) => Some(bytes)
    }
  }

  def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val entry = entries.synchronized { entries.get(blockId) }
    entry match {
      case null => None
      case e: SerializedMemoryEntry[_] =>
        throw new IllegalArgumentException("should only call getValues on deserialized blocks")
      case DeserializedMemoryEntry(values, _, _) =>
        val x = Some(values)
        x.map(_.iterator)
    }
  }

  def remove(blockId: BlockId): Boolean = memoryManager.synchronized {
    val entry = entries.synchronized {
      if (entries.containsKey(blockId)){
        entries.remove(blockId)
      }
      else {
        null
      }
    }
    if (blockId.isRDD){
      // val ref_count = currentRefMap(blockId)
      currentRefMap.synchronized {
        if (currentRefMap.contains(blockId)){
          currentRefMap.remove(blockId)
        }
      }
    }

    if (blockId.isRDD) {
      currentDAGInfoMap.synchronized {
        if (currentDAGInfoMap.contains(getRddId(blockId).get)) {
          currentDAGInfoMap.remove(getRddId(blockId).get)
        }
      }
      currentLease.synchronized {
        if (currentLease.contains(getRddId(blockId).get)) {
          currentLease.remove(getRddId(blockId).get)
        }
      }
    }

    if (entry != null) {
      entry match {
        case SerializedMemoryEntry(buffer, _, _) => buffer.dispose()
        case _ =>
      }
      memoryManager.releaseStorageMemory(entry.size, entry.memoryMode)
      logInfo(s"LRC: Block $blockId of size ${entry.size} dropped " +
        s"from memory (free ${maxMemory - blocksMemoryUsed})")
      true
    } else {
      false
    }
  }

  def clear(): Unit = memoryManager.synchronized {
    entries.synchronized {
      entries.clear()
    }
    onHeapUnrollMemoryMap.clear()
    offHeapUnrollMemoryMap.clear()
    memoryManager.releaseAllStorageMemory()
    logInfo("MemoryStore cleared")
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  /**
   * Try to evict blocks to free up a given amount of space to store a particular block.
   * Can fail if either the block is bigger than our memory or it would require replacing
   * another block from the same RDD (which leads to a wasteful cyclic replacement pattern for
   * RDDs that don't fit into memory that we want to avoid).
   *
   * @param blockId the ID of the block we are freeing space for, if any
   * @param space the size of this block
   * @param memoryMode the type of memory to free (on- or off-heap)
   * @return the amount of memory (in bytes) freed by eviction
   */
  private[spark] def evictBlocksToFreeSpace(
      blockId: Option[BlockId],
      space: Long,
      memoryMode: MemoryMode): Long = {
    assert(space > 0)
    memoryManager.synchronized {
      var freedMemory = 0L
      val rddToAdd = blockId.flatMap(getRddId)
      val selectedBlocks = new ArrayBuffer[BlockId]
      def blockIsEvictable(blockId: BlockId, entry: MemoryEntry[_]): Boolean = {
        entry.memoryMode == memoryMode && (rddToAdd.isEmpty || rddToAdd != getRddId(blockId))
      }

      logInfo(s"LRC: LRC-online: Try to evict space for $blockId")
      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
//      entries.synchronized {
//        val iterator = entries.entrySet().iterator()
//        while (freedMemory < space && iterator.hasNext) {
//          val pair = iterator.next()
//          val blockId = pair.getKey
//          val entry = pair.getValue
//          if (blockIsEvictable(blockId, entry)) {
//            // We don't want to evict blocks which are currently being read, so we need to obtain
//            // an exclusive write lock on blocks which are candidates for eviction. We perform a
//            // non-blocking "tryLock" here in order to ignore blocks which are locked for reading:
//            if (blockManager.blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
//              selectedBlocks += blockId
//              freedMemory += pair.getValue.size
//            }
//          }
//        }
//      }
      ///!!!!!!!!!!!!!!!!!!!!!!!   Above is LRU

      //var freedMemory2 = 0L
      //val selectedBlocks2 = new ArrayBuffer[BlockId]
      //var blockToCacheRefCount = Int.MaxValue
      //// yyh: if this is a broadcast block, cache it anyway
      //refMap.synchronized {
        //if (blockId.isDefined && blockId.get.isRDD){
          //if (refMap.contains(blockId.get))
          //{
            //blockToCacheRefCount = refMap(blockId.get)
            //logInfo(s"LRC: The ref count of $blockId is $blockToCacheRefCount")
          //}
          //else {
            //blockToCacheRefCount = 1
            //logError(s"LRC: The ref count of $blockId is not in the refMap")
          //}
        //}
      //}

      //currentRefMap.synchronized {
      //// Sort all the blocks in current cache by their ref counts
      //// Only rdd blocks will be put in the currentRefMap
      //val listMap = ListMap(currentRefMap.toSeq.sortBy(_._2): _*)
      //breakable {
        //for ((thisBlockId, thisRefCount) <- listMap){
          //if (entries.containsKey(thisBlockId) && blockIsEvictable(thisBlockId, entries.get(thisBlockId))) {
            //if (blockManager.blockInfoManager.lockForWriting(thisBlockId, blocking = false).isDefined) {
              //if (thisRefCount < blockToCacheRefCount && freedMemory < space) {
                //selectedBlocks += thisBlockId
                //entries.synchronized {
                  //freedMemory += entries.get(thisBlockId).size
                //}
              //}
              //else {
                //break
              //}
            //}
            //}
          //}
        //}
      //}
      //logInfo(s"LRC: To evict blocks $selectedBlocks")
      //!!!!!!!!!!!!!!!!!!!!!!!!!  ABOVE IS LRC !!!!!!!!!!!!!!!!!!!!!!!!!!!!


      currentDAGInfoMap.synchronized {
        val s = entries.asScala
        if (s.filter(x => x._1.isRDD).filter(x=> blockIsEvictable(x._1, x._2)).isEmpty) {
          logInfo("Leasing: There is no evictable blocks!!!")
        }
        val BlocksDoNotHaveALease =  s
          .keySet
          .filter( x => x.isRDD)
          .filter( x => !currentLease.contains(getRddId(x).getOrElse(0)) )
        breakable {
          for (thisblockId <- BlocksDoNotHaveALease) {
            if (entries.containsKey(thisblockId) && blockIsEvictable(thisblockId, entries.get(thisblockId))) {
              if (blockManager.blockInfoManager.lockForWriting(thisblockId, blocking = false).isDefined) {
                if (freedMemory < space) {
                  selectedBlocks += thisblockId
                  entries.synchronized {
                    freedMemory += entries.get(thisblockId).size
                  }
                } else {
                  break
                }
              }
            }
          }
        }
          if (freedMemory < space) {
            val listmap =ListMap(currentLease.toSeq.sortBy(_._2): _*)
//            breakable {
//              for ( (thisrddid, _) <- listmap) {
//                if (currentDAGInfoMap.contains(thisrddid)) {
//                  entries.synchronized {
//                    val iterator = entries.entrySet().iterator()
//                    while (freedMemory < space && iterator.hasNext) {
//                      val pair = iterator.next()
//                      val blockId = pair.getKey
//                      val entry = pair.getValue
//                      if (blockId.isRDD) {
//                        val corddid = blockId.asRDDId.toString.split("_")(1).toInt
//                        if (thisrddid == corddid ) {
//                          if ( blockIsEvictable(blockId, entry)) {
//                            if (!selectedBlocks.contains(blockId)) {
//                              if (blockManager.blockInfoManager.lockForWriting(blockId, blocking = false).isDefined) {
//                                selectedBlocks += blockId
//                                freedMemory += pair.getValue.size
//                              }
//                            }
//                          }
//                        }
//                      }
//                    }
//                  }
//                  if (freedMemory >= space) {
//                    break
//                  }
//                }
//              }
//            }
            breakable {
              for ((thisRDDId, _) <- listmap) {
                if (freedMemory >= space) {
                  break
                }
                breakable {
                  for (blockEvict <- s.keySet.filter(x => x.isRDD).filter((x => x.asRDDId.toString.split("_")(1).toInt==thisRDDId))) {
                    if (entries.containsKey(blockEvict) && blockIsEvictable(blockEvict, entries.get(blockEvict))) {
                      if (blockManager.blockInfoManager.lockForWriting(blockEvict, blocking = false).isDefined) {
                        if (currentLease.getOrElse(thisRDDId, 1) <= currentLease.getOrElse(blockEvict.asRDDId.toString.split("_")(1).toInt, 0) && freedMemory < space) {
                          selectedBlocks += blockEvict
                          entries.synchronized {
                            freedMemory += entries.get(blockEvict).size
                          }
                        } else {
                          break
                        }
                      }
                    }
                  }
                }
              }
            }
          }
      }
      logWarning(s"Leasing: The to evict block is $selectedBlocks, entries: ${entries.keySet()},the currentLease is $currentLease, the size is $freedMemory")


      def dropBlock[T](blockId: BlockId, entry: MemoryEntry[T]): Unit = {
        val data = entry match {
          case DeserializedMemoryEntry(values, _, _) => Left(values)
          case SerializedMemoryEntry(buffer, _, _) => Right(buffer)
        }
        val newEffectiveStorageLevel =
          blockEvictionHandler.dropFromMemory(blockId, () => data)(entry.classTag)
        if (newEffectiveStorageLevel.isValid) {
          // The block is still present in at least one store, so release the lock
          // but don't delete the block info
          blockManager.blockInfoManager.unlock(blockId)
        } else {
          // The block isn't present in any store, so delete the block info so that the
          // block can be stored again
          blockManager.blockInfoManager.removeBlock(blockId)
        }
      }

      if (freedMemory >= space) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping " +
          s"(${Utils.bytesToString(freedMemory)} bytes)")
        for (blockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            dropBlock(blockId, entry)
          }
        }
        logInfo(s"After dropping ${selectedBlocks.size} blocks, " +
          s"free memory is ${Utils.bytesToString(maxMemory - blocksMemoryUsed)}")
        freedMemory
      } else {
        blockId.foreach { id =>
          logInfo(s"Will not store $id")
        }
        selectedBlocks.foreach { id =>
          blockManager.blockInfoManager.unlock(id)
        }
        0L
      }
    }
  }

  def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve memory for unrolling the given block for this task.
   *
   * @return whether the request is granted.
   */
  def reserveUnrollMemoryForThisTask(
      blockId: BlockId,
      memory: Long,
      memoryMode: MemoryMode): Boolean = {
    memoryManager.synchronized {
      val success = memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)
      if (success) {
        val taskAttemptId = currentTaskAttemptId()
        val unrollMemoryMap = memoryMode match {
          case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
          case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
        }
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      success
    }
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   */
  def releaseUnrollMemoryForThisTask(memoryMode: MemoryMode, memory: Long = Long.MaxValue): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    memoryManager.synchronized {
      val unrollMemoryMap = memoryMode match {
        case MemoryMode.ON_HEAP => onHeapUnrollMemoryMap
        case MemoryMode.OFF_HEAP => offHeapUnrollMemoryMap
      }
      if (unrollMemoryMap.contains(taskAttemptId)) {
        val memoryToRelease = math.min(memory, unrollMemoryMap(taskAttemptId))
        if (memoryToRelease > 0) {
          unrollMemoryMap(taskAttemptId) -= memoryToRelease
          memoryManager.releaseUnrollMemory(memoryToRelease, memoryMode)
        }
        if (unrollMemoryMap(taskAttemptId) == 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   */
  def currentUnrollMemory: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.values.sum + offHeapUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   */
  def currentUnrollMemoryForThisTask: Long = memoryManager.synchronized {
    onHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L) +
      offHeapUnrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   */
  private def numTasksUnrolling: Int = memoryManager.synchronized {
    (onHeapUnrollMemoryMap.keys ++ offHeapUnrollMemoryMap.keys).toSet.size
  }

  /**
   * Log information about current memory usage.
   */
  private def logMemoryUsage(): Unit = {
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemoryUsed)} (blocks) + " +
      s"${Utils.bytesToString(currentUnrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(memoryUsed)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }
  /**
   * yyh
   * Deduct the referenced count of the given blockId by 1
   */
  def deductRefCountByBlockIdMiss(blockId: BlockId): Unit = refMap.synchronized {
    refMap.synchronized {
      refMap(blockId) -= 1
      val newRefCount = refMap(blockId)
      logInfo(s"yyh: ref count of $blockId is deducted to $newRefCount")
    }
  }

  /**
   * yyh Cache Hit
   * Deduct the referenced count of the given blockId by 1
   */
  def deductRefCountByBlockIdHit(blockId: BlockId): Unit = {
    refMap.synchronized{
      refMap(blockId) -= 1
      val newRefCount = refMap(blockId)
      logWarning(s"LRC: ref count of $blockId is deducted to $newRefCount")
    }
    currentRefMap.synchronized{ currentRefMap(blockId) -= 1}
  }
  /**
   * yyh for conservative all-or-nothing
   * decrease the ref count of peers on eviction of the given block
   * This is only triggered by the message from the driver
   * What if the particular block has not been generated: keep a record in the peerLostBlocks
   */
  def checkPeersConservatively(blockId: BlockId): Unit = refMap.synchronized {
    refMap.synchronized {
      if (refMap.contains(blockId)) {
        refMap(blockId) -= 1
        logInfo(s"yyh: ref count of $blockId is deducted to ${refMap(blockId)}" +
          s"because of conservative all-or-nothing")
        // This block must have been generated somewhere,
        // so no need to record it in the peerLostBlocks
      }
    }
    currentRefMap.synchronized{
      if (currentRefMap.contains(blockId)){
        currentRefMap(blockId) -= 1
      }
    }

    val rddId = blockId.asRDDId.toString.split("_")(1).toInt // rdd_1_1
    val index = blockId.asRDDId.toString.split("_")(2).toInt
    if (blockManager.peers.contains(rddId))
    {
      val peerRDDId = blockManager.peers.get(rddId).get
      val peerBlockId = new RDDBlockId(peerRDDId, index)
      refMap.synchronized {
        if (refMap.contains(peerBlockId)) {
          refMap(peerBlockId) -= 1
          logInfo(s"yyh: ref count of $peerBlockId is deducted to ${refMap(peerBlockId)} " +
            s"because of conservative all-or-nothing")

        }
        else {
          blockManager.peerLostBlocks += peerBlockId
          // The peer block is not in the worker, record it in case it is cached in the future
          logInfo(s"yyh: $peerBlockId is added to the peerLostBlocks: ")
        }
      }
      currentRefMap.synchronized {
        if (currentRefMap.contains(peerBlockId)) {
          currentRefMap(peerBlockId) -= 1
        }
      }
    }

  }
  /**
   * yyh for strict all-or-nothing
   * decrease the ref count of peers on eviction of the given block
   * This is only triggered by the message from the driver
   * what if the peer blocks have not yet been generated ?
   * Notify the blockManager to edit its refProfile
   */
  def checkPeersStrictly(blockId: BlockId): Unit = refMap.synchronized {
    val rddId = blockId.asRDDId.toString.split("_")(1).toInt
    decreaseRDDRefCount(rddId)
    val peerRDDId = blockManager.peers.get(rddId)
    if (peerRDDId.isDefined) {
      decreaseRDDRefCount(peerRDDId.get)
    }
  }

  def decreaseRDDRefCount(rddId: Int): Unit = {
    if (blockManager.refProfile.contains(rddId)){
      blockManager.refProfile(rddId) -= 1
      logInfo(s"yyh: the ref count of $rddId in blockManager's refProfile is deducted " +
        s"to ${blockManager.refProfile(rddId)} due to strict all-or-nothing")
    }
    if (blockManager.refProfile_online.contains(rddId)){
      blockManager.refProfile_online(rddId) -= 1
      logInfo(s"yyh: the ref count of $rddId in blockManager's refProfile_online is deducted " +
        s"to ${blockManager.refProfile_online(rddId)} due to strict all-or-nothing")
    }
    refMap.synchronized {
      refMap.foreach{ case (key: BlockId, value: Int) =>
        if (key.asRDDId.toString.split("_")(1).toInt == rddId) {
          logInfo(s"yyh: ref count of $key id is deducted to ${value-1}" +
            s"because of strict all-or-nothing")
          (key, value-1)
        }
      }
    }
    currentRefMap.synchronized{
      currentRefMap.foreach{ case (key: BlockId, value: Int) =>
        if (key.asRDDId.toString.split("_")(1).toInt == rddId) {
          logInfo(s"yyh: ref count of $key id is deducted to ${value-1}" +
            s"because of strict all-or-nothing")
          (key, value-1)
        }
      }
    }
  }

  /**
  filter blocks by rddId. If the rddId is in the jobDAG profile, update the block ref count.
   */
  private def updateFilter(blockId: BlockId, origin: Int, jobDAG: mutable.HashMap[Int, Int]) = {
    val rddId = blockId.asRDDId.toString.split("_")(1).toInt
    jobDAG.getOrElse(rddId, origin)
    // if to be updated, just replace it. No need to add, as different tenants will not share RDDs
    // and there is no parallel jobs for any single tenant.
    // For an RDD to be used in the next job, its origin ref count might be 1.
  }

  private def updateDAGFilter(blockId: Int, origin: mutable.HashMap[Int, Int], daginfo: mutable.HashMap[Int, mutable.HashMap[Int, Int]]) = {
    daginfo.getOrElse(blockId, origin)
  }


  /**
  Update both refMap and currentRefMap with the received jobDAG in BlockManager
   */
  def updateRefCountByJobDAG(jobDAG: mutable.HashMap[Int, Int]): Unit = {
    logInfo(s"yyh: Update ref maps on receiving job DAG: $jobDAG")

    // yyh!!! comment below for LRC with app-DAG
    logInfo(s"yyh: before: currentRefMap: $currentRefMap")
    refMap.synchronized {
      refMap = refMap.map{ case (k, v) => (k, updateFilter(k, v, jobDAG))}
    }
    currentRefMap.synchronized{
      currentRefMap = currentRefMap.map{ case (k, v) => (k, updateFilter(k, v, jobDAG))}
    }
    logInfo(s"yyh: after: currentRefMap: $currentRefMap")

  }

  /**
   Update the DAGInfo for this Job. the DAGInfo is from the BlockManager
   *
   **/
  def updateDAGInfoThisJob(DAGInfo: mutable.HashMap[Int, mutable.HashMap[Int, Int]], accessNumber: Int): Unit = {
    logInfo(s"Leasing: Update DAGInfo on receiveing jobDAG $DAGInfo")

    logInfo(s"Leasing: before: currentDAGInfoMap: $currentDAGInfoMap , access number: $accessNumber ")
    logInfo(s"Leasing: before: DAGInfoMap: $DAGInfoMap , access number: $accessNumber ")

    globalDAG.synchronized {
      for ((blockid, riAndfreq) <- DAGInfo) {
        globalDAG.put(blockid, updateDAGFilter(blockid, riAndfreq, DAGInfo))
      }
    }

    DAGInfoMap.synchronized {
      DAGInfoMap = mutable.HashMap[Int, mutable.HashMap[Int, Int]]()
      for ((blockid, riAndfreq) <- DAGInfo) {
        DAGInfoMap.put(blockid, updateDAGFilter(blockid, riAndfreq, DAGInfo))
      }
    }

    currentDAGInfoMap.synchronized {
      currentDAGInfoMap = mutable.HashMap[Int, mutable.HashMap[Int, Int]]()
//      for ( (blockid, riAndfreq) <- DAGInfo) {
//        currentDAGInfoMap.put(blockid, updateDAGFilter(blockid, riAndfreq, DAGInfo))
//      }
    }
    DAGInfoMap = DAGInfoMap.filter(kv => kv._2.nonEmpty)
    AccessNumberGlobal = accessNumber
    logInfo(s"Leasing: after: currentDAGInfoMap: $currentDAGInfoMap , access number: $accessNumber ")
    logInfo(s"Leasing: after: DAGInfoMap: $DAGInfoMap , access number: $accessNumber ")

    logWarning(s"Leasing: before OSL, leaseMAP $leaseMap")
    OSL()
    logWarning(s"Leasing: after OSL, leaseMAP $leaseMap")
  }


  /**
   * Log a warning for failing to unroll a block.
   *
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  private def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsValues()]] call.
 *
 * @param memoryStore  the memoryStore, used for freeing memory.
 * @param memoryMode   the memory mode (on- or off-heap).
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param unrolled     an iterator for the partially-unrolled values.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 */
private[storage] class PartiallyUnrolledIterator[T](
    memoryStore: MemoryStore,
    memoryMode: MemoryMode,
    unrollMemory: Long,
    private[this] var unrolled: Iterator[T],
    rest: Iterator[T])
  extends Iterator[T] {

  private def releaseUnrollMemory(): Unit = {
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    // SPARK-17503: Garbage collects the unrolling memory before the life end of
    // PartiallyUnrolledIterator.
    unrolled = null
  }

  override def hasNext: Boolean = {
    if (unrolled == null) {
      rest.hasNext
    } else if (!unrolled.hasNext) {
      releaseUnrollMemory()
      rest.hasNext
    } else {
      true
    }
  }

  override def next(): T = {
    if (unrolled == null) {
      rest.next()
    } else {
      unrolled.next()
    }
  }

  /**
   * Called to dispose of this iterator and free its memory.
   */
  def close(): Unit = {
    if (unrolled != null) {
      releaseUnrollMemory()
    }
  }
}

/**
 * A wrapper which allows an open [[OutputStream]] to be redirected to a different sink.
 */
private[storage] class RedirectableOutputStream extends OutputStream {
  private[this] var os: OutputStream = _
  def setOutputStream(s: OutputStream): Unit = { os = s }
  override def write(b: Int): Unit = os.write(b)
  override def write(b: Array[Byte]): Unit = os.write(b)
  override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
  override def flush(): Unit = os.flush()
  override def close(): Unit = os.close()
}

/**
 * The result of a failed [[MemoryStore.putIteratorAsBytes()]] call.
 *
 * @param memoryStore the MemoryStore, used for freeing memory.
 * @param serializerManager the SerializerManager, used for deserializing values.
 * @param blockId the block id.
 * @param serializationStream a serialization stream which writes to [[redirectableOutputStream]].
 * @param redirectableOutputStream an OutputStream which can be redirected to a different sink.
 * @param unrollMemory the amount of unroll memory used by the values in `unrolled`.
 * @param memoryMode whether the unroll memory is on- or off-heap
 * @param bbos byte buffer output stream containing the partially-serialized values.
 *                     [[redirectableOutputStream]] initially points to this output stream.
 * @param rest         the rest of the original iterator passed to
 *                     [[MemoryStore.putIteratorAsValues()]].
 * @param classTag the [[ClassTag]] for the block.
 */
private[storage] class PartiallySerializedBlock[T](
    memoryStore: MemoryStore,
    serializerManager: SerializerManager,
    blockId: BlockId,
    private val serializationStream: SerializationStream,
    private val redirectableOutputStream: RedirectableOutputStream,
    val unrollMemory: Long,
    memoryMode: MemoryMode,
    bbos: ChunkedByteBufferOutputStream,
    rest: Iterator[T],
    classTag: ClassTag[T]) {

  private lazy val unrolledBuffer: ChunkedByteBuffer = {
    bbos.close()
    bbos.toChunkedByteBuffer
  }

  // If the task does not fully consume `valuesIterator` or otherwise fails to consume or dispose of
  // this PartiallySerializedBlock then we risk leaking of direct buffers, so we use a task
  // completion listener here in order to ensure that `unrolled.dispose()` is called at least once.
  // The dispose() method is idempotent, so it's safe to call it unconditionally.
  Option(TaskContext.get()).foreach { taskContext =>
    taskContext.addTaskCompletionListener { _ =>
      // When a task completes, its unroll memory will automatically be freed. Thus we do not call
      // releaseUnrollMemoryForThisTask() here because we want to avoid double-freeing.
      unrolledBuffer.dispose()
    }
  }

  // Exposed for testing
  private[storage] def getUnrolledChunkedByteBuffer: ChunkedByteBuffer = unrolledBuffer

  private[this] var discarded = false
  private[this] var consumed = false

  private def verifyNotConsumedAndNotDiscarded(): Unit = {
    if (consumed) {
      throw new IllegalStateException(
        "Can only call one of finishWritingToStream() or valuesIterator() and can only call once.")
    }
    if (discarded) {
      throw new IllegalStateException("Cannot call methods on a discarded PartiallySerializedBlock")
    }
  }

  /**
   * Called to dispose of this block and free its memory.
   */
  def discard(): Unit = {
    if (!discarded) {
      try {
        // We want to close the output stream in order to free any resources associated with the
        // serializer itself (such as Kryo's internal buffers). close() might cause data to be
        // written, so redirect the output stream to discard that data.
        redirectableOutputStream.setOutputStream(ByteStreams.nullOutputStream())
        serializationStream.close()
      } finally {
        discarded = true
        unrolledBuffer.dispose()
        memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
      }
    }
  }

  /**
   * Finish writing this block to the given output stream by first writing the serialized values
   * and then serializing the values from the original input iterator.
   */
  def finishWritingToStream(os: OutputStream): Unit = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    ByteStreams.copy(unrolledBuffer.toInputStream(dispose = true), os)
    memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory)
    redirectableOutputStream.setOutputStream(os)
    while (rest.hasNext) {
      serializationStream.writeObject(rest.next())(classTag)
    }
    serializationStream.close()
  }

  /**
   * Returns an iterator over the values in this block by first deserializing the serialized
   * values and then consuming the rest of the original input iterator.
   *
   * If the caller does not plan to fully consume the resulting iterator then they must call
   * `close()` on it to free its resources.
   */
  def valuesIterator: PartiallyUnrolledIterator[T] = {
    verifyNotConsumedAndNotDiscarded()
    consumed = true
    // Close the serialization stream so that the serializer's internal buffers are freed and any
    // "end-of-stream" markers can be written out so that `unrolled` is a valid serialized stream.
    serializationStream.close()
    // `unrolled`'s underlying buffers will be freed once this input stream is fully read:
    val unrolledIter = serializerManager.dataDeserializeStream(
      blockId, unrolledBuffer.toInputStream(dispose = true))(classTag)
    // The unroll memory will be freed once `unrolledIter` is fully consumed in
    // PartiallyUnrolledIterator. If the iterator is not consumed by the end of the task then any
    // extra unroll memory will automatically be freed by a `finally` block in `Task`.
    new PartiallyUnrolledIterator(
      memoryStore,
      memoryMode,
      unrollMemory,
      unrolled = unrolledIter,
      rest = rest)
  }
}
