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

package org.apache.spark.storage

import java.io.FileWriter
import java.nio.file.{Files, Paths}
import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.immutable.List
import scala.collection.mutable.HashMap
import scala.io.Source

/**
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus)
  extends ThreadSafeRpcEndpoint with Logging {

  // Mapping from block manager id to the block manager's information.
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]

  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  private val startTime = System.currentTimeMillis
  logInfo(s"yyh: Log start time: $startTime")
  var RDDHit = 0
  var RDDMiss = 0
  var diskRead = 0
  var diskWrite = 0
  var totalReference = 0
  private val refProfile = mutable.HashMap[Int, Int]() // yyh
  private val refProfile_By_Job = mutable.HashMap[Int, mutable.HashMap[Int, Int]]()
  private val appName = conf.getAppName.filter(!" ".contains(_))
  val path = System.getProperty("user.dir")
  val appDAG = path + "/" + appName + ".txt"
  logInfo(s"LRC: Driver Endpoint tries to read profile: path: $appDAG")
  if (Files.exists(Paths.get(appDAG))) {
    for (line <- Source.fromFile(appDAG).getLines()) {
      val z = line.split(":")
      refProfile(z(0).toInt) = z(1).toInt
    }
    // refProfile(-1) = Int.MaxValue
  }

  val jobDAG = path + "/" + appName + "-JobDAG.txt"
  logInfo(s"LRC: Driver Endpoint tries to read profile by job: path: $jobDAG")
  if (Files.exists(Paths.get(jobDAG))) {
    for (line <- Source.fromFile(jobDAG).getLines()) {
      val z = line.split("-")
      val jobId = z(0).toInt
      val this_refProfile = mutable.HashMap[Int, Int]()
      if (z.length > 1) {
        // some jobs may have no rdd refs
        val refs = z(1).split(";")
        for (ref <- refs) {
          val pairs = ref.split(":")
          this_refProfile(pairs(0).toInt) = pairs(1).toInt
        }
      }
      refProfile_By_Job(jobId) = this_refProfile
    }
  }
  /** for all-or-nothing property */

  private val peerProfile = mutable.HashMap[Int, Int]()
  // Notice that each rdd has at most one peer rdd, as no operation handles more than two RDDs
  // Be careful, here we only assume that all the peers are only required once.
  // That means once either of the peer got evicted, it is safe to clear the ref count of the other
  // for all-or-nothing considerations.
  // It's the BlockManager on slaves who decide to report an eviction of a block with a peer or not.
  // In the case where a block whose peer is already evicted, the BlockManger should not report.

  val peers = path + "/" + appName + "-Peers.txt"
  logInfo(s"yyh: Driver Endpoint tries to read peers profile: path :$peers")
  if (Files.exists(Paths.get(peers))) {
    for (line <- Source.fromFile(peers).getLines()) {
      val z = line.split(":")
      peerProfile(z(0).toInt) = z(1).toInt // mutual peers, as later we only search by key
      peerProfile(z(1).toInt) = z(0).toInt
    }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) =>
      register(blockManagerId, maxMemSize, slaveEndpoint)
      context.reply(true)

    case _updateBlockInfo @
        UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      context.reply(updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size))
      listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))

    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))

    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))

    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))

    case GetExecutorEndpointRef(executorId) =>
      context.reply(getExecutorEndpointRef(executorId))

    case GetMemoryStatus =>
      context.reply(memoryStatus)

    case GetStorageStatus =>
      context.reply(storageStatus)

    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))

    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))

    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))

    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))

    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))

    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()

    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }

    case StartBroadcastJobId(jobId) =>
      broadcastJobDAG(jobId) // , refProfile_By_Job(jobId))
      context.reply(true)
    case ReportCacheHit(blockManagerId, list) => // yyh
      updateCacheHit(blockManagerId, list)
      context.reply(true)

    case GetRefProfile(blockManagerId, slaveEndPoint) => // yyh
      context.reply(getRefProfile(blockManagerId, slaveEndPoint))

    case BlockWithPeerEvicted(blockId) => // yyh
      onPeerEvicted(blockId)
      context.reply(true)

    case StartBroadcastRefCount(jobId, partitionNumber, refCount) =>
      broadcastJobDAG(jobId, partitionNumber, refCount)
      context.reply(true)

    case StartBroadcastDAGInfo(jobId, partitionNumber, a, b) =>
      broadcastDAGInfo(jobId, partitionNumber, a, b)
      context.reply(true)
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      if (locations.size == 0) {
        blockLocations.remove(blockId)
      }
    }
    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId) {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks.asScala)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.asScala.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  private def register(id: BlockManagerId, maxMemSize: Long, slaveEndpoint: RpcEndpointRef) {
    val time = System.currentTimeMillis()
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), id))

      blockManagerIdByExecutor(id.executorId) = id

      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveEndpoint)
    }
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxMemSize))
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns an [[RpcEndpointRef]] of the [[BlockManagerSlaveEndpoint]] for sending RPC messages.
   */
  private def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      info.slaveEndpoint
    }
  }

  private def broadcastJobDAG(jobId: Int): Unit = {
    for (bm <- blockManagerInfo.values) {
      val (currentRefMap, refMap) = bm.slaveEndpoint.askWithRetry[(mutable.Map[BlockId, Int],
        mutable.Map[BlockId, Int])](BroadcastJobDAG(jobId, None))
      // val (currentRefMap, refMap) = bm.broadcastJobDAG(jobId)
      logInfo(s"LRC: Updated CurrentRefMap from $bm: $currentRefMap")
      logInfo(s"LRC: Updated RefMap from $bm: $refMap")
    }
  }

  private def broadcastJobDAG(jobId: Int, partitionNumber: Int,
                              refCount: mutable.HashMap[Int, Int]): Unit = {
    logInfo(s"LRC: Start to broadcast the profiled refCount of job $jobId")
    logInfo(s"$refCount")
    for (bm <- blockManagerInfo.values) {
      val (currentRefMap, refMap) = bm.slaveEndpoint.askWithRetry[(mutable.HashMap[BlockId, Int],
        mutable.HashMap[BlockId, Int])](BroadcastJobDAG(jobId, Some(refCount)))
      // val (currentRefMap, refMap) = bm.broadcastJobDAG(jobId)
      logInfo(s"LRC: Updated CurrentRefMap from $bm: $currentRefMap")
      logInfo(s"LRC: Updated RefMap from $bm: $refMap")
    }
    // update the total reference count.
    this.synchronized{totalReference += refCount.foldLeft(0)(_ + _._2) * partitionNumber}
  }

  private def broadcastDAGInfo(jobId: Int, partitionNumber: Int,
                               DAGInfo: HashMap[Int, HashMap[Int, Int]], AccessNumber: Int): Unit = {
    logWarning(s"Leasing: Start to broadcast the DAGInfo of Job $jobId")
    logInfo(s"Leasing: DAGInfo: $DAGInfo")
    for (bm <- blockManagerInfo.values) {
      val (currentDAGInfo, dagInfo, currentAccessnumber) = bm.slaveEndpoint.askWithRetry[(mutable.HashMap[Int, mutable.HashMap[Int, Int]], mutable.HashMap[Int,
        mutable.HashMap[Int, Int]], Int)](BroadcastDAGInfo(jobId, Some(DAGInfo), AccessNumber))
      logInfo(s"Leasing: Update Current")
    }
  }

  /**
  private def broadcastRefCount(refCount: mutable.HashMap[Int, Int]): Unit = {
    for (bm <- blockManagerInfo.values) {
      bm.slaveEndpoint.askWithRetry(BroadcastRefCount(refCount))
      logInfo(s"zcl: broadcasted refcount to $bm")
    }
  }
   */

  private def updateCacheHit(blockManagerId: BlockManagerId, list: List[Int]):
  Boolean = {
    // list (hitCount, missCount, diskRead, diskWrite)
    this.synchronized{
      RDDHit += list(0)
      RDDMiss += list(1)
      diskRead += list(2)
      diskWrite += list(3)

    }
    logDebug(s"LRC: Received Report from $blockManagerId: " +
      s"RDD Hit count increased by ${list(0)}. now $RDDHit" +
      s"RDD Miss count increased by ${list(1)}. now $RDDMiss" +
      s"Disk Read count increased by ${list(2)}. now $diskRead" +
      s"Disk Write count increased by ${list(3)}. now $diskWrite"
    )
    true
  }

  private def getRefProfile(blockManagerId: BlockManagerId, slaveEndPoint: RpcEndpointRef):
  (mutable.HashMap[Int, Int], mutable.HashMap[Int, mutable.HashMap[Int, Int]],
    mutable.HashMap[Int, Int]) = {
    logDebug(s"LRC: Got the request of refProfile from block manager $blockManagerId, responding")
    (refProfile, refProfile_By_Job, peerProfile)
  }

  private def onPeerEvicted(blockId: BlockId): Unit = {
    val rddId = blockId.asRDDId.toString.split("_")(1).toInt
    // val index = blockId.asRDDId.toString.split("_")(2).toInt
    val peerRDDId = peerProfile.get(rddId)

    if (peerRDDId.isEmpty) {
      logError(s"LRC: The reported block $blockId has no peer!")
    }
    else {
      // For conservative all-or-nothing, decrease the ref count of the corresponding block
      // val peerBlockId = new RDDBlockId(peerRDDId.get, index)
      notifyPeersConservatively(blockId)

      // For strict all-or-nothing, decrease the ref count of all the blocks of both rdds
      notifyPeersStrictly(blockId)


    }
  }
  private def notifyPeersConservatively(blockId: BlockId): Unit = {
    for (bm <- blockManagerInfo.values) {
      bm.slaveEndpoint.ask[Boolean](CheckPeersConservatively(blockId))
    }

    /**
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // yyh: tell the blockManager to decrease the ref count of the given block
          logInfo(s"yyh: Telling $blockManager to decrease the ref count of $blockId")
          blockManager.get.slaveEndpoint.ask[Boolean](DecreaseBlockRefCount(blockId))
        }
      }
    }
     */
  }
  private def notifyPeersStrictly(blockId: BlockId): Unit = {
    for (bm <- blockManagerInfo.values) {
      bm.slaveEndpoint.ask[Boolean](CheckPeersStrictly(blockId))
    }

    /** TRY TO FIND THE LOCATION OF EACH BLOCK.
     * But we are not sure whether all the block status are reported to the master
    val blocks = blockLocations.asScala.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach{
        bm =>
          {
            blockManagerInfo.get(bm)
              .get.slaveEndpoint.ask[Boolean](DecreaseBlockRefCount(blockId))
            logInfo(s"yyh: Telling $bm to decrease the ref count of $blockId")
          }
      }

    }
     */
  }

  override def onStop(): Unit = {

    val stopTime = System.currentTimeMillis
    val duration = stopTime - startTime
//    RDDHit = totalReference - RDDMiss // yyh: align total reference count
//    if (RDDHit < 0 ){
//      RDDHit = 0
//    }
    logInfo(s"LRC: log stoptime: $stopTime, duration: $duration ms")
    logInfo(s"LRC: Closing blockMangerMasterEndPoint, RDD hit $RDDHit, RDD miss $RDDMiss")
    logInfo(s"LRC: Disk read count: $diskRead, disk write count: $diskWrite")
    // val path = System.getProperty("user.dir")
    val appName = conf.getAppName
    val fw = new FileWriter("result.txt", true) // true means append mode
    fw.write(s"AppName: $appName, Runtime: $duration\n")
    fw.write(s"RDD Hit\t$RDDHit\tRDD Miss\t$RDDMiss\n")
    fw.close()
    askThreadPool.shutdownNow()
  }
}

@DeveloperApi
case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long) {
  def isCached: Boolean = memSize + diskSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, memSize = 0L, diskSize = 0L)
}

private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxMem: Long,
    val slaveEndpoint: RpcEndpointRef)
  extends Logging {

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long) {

    updateLastSeenMs()

    if (_blocks.containsKey(blockId)) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      val originalLevel: StorageLevel = blockStatus.storageLevel
      val originalMemSize: Long = blockStatus.memSize

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory or on-disk.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize = memSize, diskSize = 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, memSize = 0, diskSize = diskSize)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }
      if (!blockId.isBroadcast && blockStatus.isCached) {
        _cachedBlocks += blockId
      }
    } else if (_blocks.containsKey(blockId)) {
      // If isValid is not true, drop the block.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
    }
  }

  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
    }
    _cachedBlocks -= blockId
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
