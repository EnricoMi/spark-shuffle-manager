/*
 * Copyright 2025 G-Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.metrics.source.Source
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.StreamCallbackWithID
import org.apache.spark.network.shuffle.MergedBlockMeta
import org.apache.spark.network.shuffle.checksum.Cause
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.{MigratableResolver, ShuffleWriteMetricsReporter}
import org.apache.spark.util.io.ChunkedByteBuffer

import java.io.File
import scala.reflect.ClassTag

class DfsBlockManager(blockManager: BlockManager, blockTransferService: BlockTransferService)
    extends BlockManager(
      blockManager.executorId,
      SparkEnv.get.rpcEnv,
      blockManager.master,
      blockManager.serializerManager,
      blockManager.conf,
      SparkEnv.get.memoryManager,
      SparkEnv.get.mapOutputTracker,
      SparkEnv.get.shuffleManager,
      blockTransferService,
      SparkEnv.get.securityManager,
      None
    ) {
  override val externalShuffleServiceEnabled: Boolean = blockManager.externalShuffleServiceEnabled
  override val subDirsPerLocalDir: Int = blockManager.subDirsPerLocalDir
  override val diskBlockManager: DiskBlockManager = blockManager.diskBlockManager
  override val blockInfoManager: BlockInfoManager = blockManager.blockInfoManager
  override val memoryStore = blockManager.memoryStore
  override val diskStore = blockManager.diskStore
  override val externalShuffleServicePort = blockManager.externalShuffleServicePort
  blockManagerId = blockManager.blockManagerId
  shuffleServerId = blockManager.shuffleServerId
  // NOTE: do not override blockStoreClient implementation
  // override val blockStoreClient = blockManager.blockStoreClient
  override val remoteBlockTempFileManager = blockManager.remoteBlockTempFileManager
  hostLocalDirManager = blockManager.hostLocalDirManager

  override private[storage] lazy val migratableResolver: MigratableResolver = blockManager.migratableResolver

  override def getLocalDiskDirs: Array[String] = blockManager.getLocalDiskDirs

  override def diagnoseShuffleBlockCorruption(blockId: BlockId, checksumByReader: Long, algorithm: String): Cause =
    blockManager.diagnoseShuffleBlockCorruption(blockId, checksumByReader, algorithm)

  override def initialize(appId: String): Unit = blockManager.initialize(appId)

  override def shuffleMetricsSource: Source = blockManager.shuffleMetricsSource

  override def reregister(): Unit = blockManager.reregister()

  override def waitForAsyncReregister(): Unit = blockManager.waitForAsyncReregister()

  override def getHostLocalShuffleData(blockId: BlockId, dirs: Array[String]): ManagedBuffer =
    blockManager.getHostLocalShuffleData(blockId, dirs)

  override def getLocalBlockData(blockId: BlockId): ManagedBuffer = blockManager.getLocalBlockData(blockId)

  override def putBlockData(
      blockId: BlockId,
      data: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]
  ): Boolean =
    blockManager.putBlockData(blockId, data, level, classTag)

  override def putBlockDataAsStream(
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[_]
  ): StreamCallbackWithID =
    blockManager.putBlockDataAsStream(blockId, level, classTag)

  override def getLocalMergedBlockData(blockId: ShuffleMergedBlockId, dirs: Array[String]): Seq[ManagedBuffer] =
    blockManager.getLocalMergedBlockData(blockId, dirs)

  override def getLocalMergedBlockMeta(blockId: ShuffleMergedBlockId, dirs: Array[String]): MergedBlockMeta =
    blockManager.getLocalMergedBlockMeta(blockId, dirs)

  override def getStatus(blockId: BlockId): Option[BlockStatus] = blockManager.getStatus(blockId)

  override def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = blockManager.getMatchingBlockIds(filter)

  override private[spark] def reportBlockStatus(blockId: BlockId, status: BlockStatus, droppedMemorySize: Long): Unit =
    blockManager.reportBlockStatus(blockId, status, droppedMemorySize)

  override def getLocalValues(blockId: BlockId): Option[BlockResult] = blockManager.getLocalValues(blockId)

  override def getLocalBytes(blockId: BlockId): Option[BlockData] = blockManager.getLocalBytes(blockId)

  override private[spark] def getRemoteValues[T: ClassTag](blockId: BlockId): Option[BlockResult] =
    blockManager.getRemoteValues(blockId)

  override private[spark] def getRemoteBlock[T](blockId: BlockId, bufferTransformer: ManagedBuffer => T): Option[T] =
    blockManager.getRemoteBlock(blockId, bufferTransformer)

  override private[spark] def sortLocations(locations: Seq[BlockManagerId]): Seq[BlockManagerId] =
    blockManager.sortLocations(locations)

  override private[spark] def readDiskBlockFromSameHostExecutor(
      blockId: BlockId,
      localDirs: Array[String],
      blockSize: Long
  ): Option[ManagedBuffer] =
    blockManager.readDiskBlockFromSameHostExecutor(blockId, localDirs, blockSize)

  override def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = blockManager.getRemoteBytes(blockId)

  override def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = blockManager.get(blockId)

  override def downgradeLock(blockId: BlockId): Unit = blockManager.downgradeLock(blockId)

  override def releaseLock(blockId: BlockId, taskContext: Option[TaskContext]): Unit =
    blockManager.releaseLock(blockId, taskContext)

  override def registerTask(taskAttemptId: Long): Unit = blockManager.registerTask(taskAttemptId)

  override def releaseAllLocksForTask(taskAttemptId: Long): Seq[BlockId] =
    blockManager.releaseAllLocksForTask(taskAttemptId)

  override def getOrElseUpdateRDDBlock[T](
      taskId: Long,
      blockId: RDDBlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]
  ): Either[BlockResult, Iterator[T]] =
    blockManager.getOrElseUpdateRDDBlock(taskId, blockId, level, classTag, makeIterator)

  override def putIterator[T: ClassTag](
      blockId: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      tellMaster: Boolean
  ): Boolean =
    blockManager.putIterator(blockId, values, level, tellMaster)

  override def getDiskWriter(
      blockId: BlockId,
      file: File,
      serializerInstance: SerializerInstance,
      bufferSize: Int,
      writeMetrics: ShuffleWriteMetricsReporter
  ): DiskBlockObjectWriter =
    blockManager.getDiskWriter(blockId, file, serializerInstance, bufferSize, writeMetrics)

  override def putBytes[T: ClassTag](
      blockId: BlockId,
      bytes: ChunkedByteBuffer,
      level: StorageLevel,
      tellMaster: Boolean
  ): Boolean =
    blockManager.putBytes(blockId, bytes, level, tellMaster)

  override private[storage] def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = blockManager.getPeers(forceFetch)

  override def replicateBlock(
      blockId: BlockId,
      existingReplicas: Set[BlockManagerId],
      maxReplicas: Int,
      maxReplicationFailures: Option[Int]
  ): Boolean =
    blockManager.replicateBlock(blockId, existingReplicas, maxReplicas, maxReplicationFailures)

  override def getSingle[T: ClassTag](blockId: BlockId): Option[T] = blockManager.getSingle(blockId)

  override def putSingle[T: ClassTag](blockId: BlockId, value: T, level: StorageLevel, tellMaster: Boolean): Boolean =
    blockManager.putSingle(blockId, value, level, tellMaster)

  private[storage] override def dropFromMemory[T: ClassTag](
      blockId: BlockId,
      data: () => Either[Array[T], ChunkedByteBuffer]
  ): StorageLevel = blockManager.dropFromMemory(blockId, data)

  override def removeRdd(rddId: Int): Int = blockManager.removeRdd(rddId)

  override def decommissionBlockManager(): Unit = blockManager.decommissionBlockManager()

  override private[spark] def decommissionSelf(): Unit = synchronized {
    blockManager.decommissionSelf()
    decommissioner = blockManager.decommissioner
  }

  override private[spark] def lastMigrationInfo(): (Long, Boolean) = blockManager.lastMigrationInfo()

  override private[storage] def getMigratableRDDBlocks(): Seq[BlockManagerMessages.ReplicateBlock] =
    blockManager.getMigratableRDDBlocks()

  override def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int =
    blockManager.removeBroadcast(broadcastId, tellMaster)

  override def removeCache(userId: String, sessionId: String): Int = blockManager.removeCache(userId, sessionId)

  override def removeBlock(blockId: BlockId, tellMaster: Boolean): Unit = blockManager.removeBlock(blockId, tellMaster)

  override def releaseLockAndDispose(blockId: BlockId, data: BlockData, taskContext: Option[TaskContext]): Unit =
    blockManager.releaseLockAndDispose(blockId, data, taskContext)

  override def stop(): Unit = blockManager.stop()
}
