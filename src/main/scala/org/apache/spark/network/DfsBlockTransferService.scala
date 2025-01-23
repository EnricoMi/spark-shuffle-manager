/*
 * Copyright 2024 G-Research
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

package org.apache.spark.network

import com.codahale.metrics.MetricSet
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.APP_ATTEMPT_ID
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.checksum.Cause
import org.apache.spark.network.shuffle._
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkEnv, SparkException}

import java.io.DataInputStream
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

class DfsBlockTransferService(conf: SparkConf, blockTransferService: BlockTransferService)
    extends BlockTransferService
    with Logging {

  appId = conf.getAppId

  override def init(blockDataManager: BlockDataManager): Unit = blockTransferService.init(blockDataManager)

  override def port: Int = blockTransferService.port

  override def hostName: String = blockTransferService.hostName

  override def setAppAttemptId(appAttemptId: String): Unit = blockTransferService.setAppAttemptId(appAttemptId)

  override def getAppAttemptId: String = blockTransferService.getAppAttemptId

  override def uploadBlock(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]
  ): Future[Unit] =
    blockTransferService.uploadBlock(hostName, port, execId, blockId, blockData, level, classTag)

  override def uploadBlockSync(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      blockData: ManagedBuffer,
      level: StorageLevel,
      classTag: ClassTag[_]
  ): Unit =
    blockTransferService.uploadBlockSync(hostname, port, execId, blockId, blockData, level, classTag)

  override def pushBlocks(
      host: String,
      port: Int,
      blockIds: Array[String],
      buffers: Array[ManagedBuffer],
      listener: BlockPushingListener
  ): Unit =
    blockTransferService.pushBlocks(host, port, blockIds, buffers, listener)

  override def fetchBlockSync(
      host: String,
      port: Int,
      execId: String,
      blockId: String,
      tempFileManager: DownloadFileManager
  ): ManagedBuffer =
    blockTransferService.fetchBlockSync(host, port, execId, blockId, tempFileManager)

  override def finalizeShuffleMerge(
      host: String,
      port: Int,
      shuffleId: Int,
      shuffleMergeId: Int,
      listener: MergeFinalizerListener
  ): Unit =
    blockTransferService.finalizeShuffleMerge(host, port, shuffleId, shuffleMergeId, listener)

  override def getMergedBlockMeta(
      host: String,
      port: Int,
      shuffleId: Int,
      shuffleMergeId: Int,
      reduceId: Int,
      listener: MergedBlocksMetaListener
  ): Unit =
    blockTransferService.getMergedBlockMeta(host, port, shuffleId, shuffleMergeId, reduceId, listener)

  override def removeShuffleMerge(host: String, port: Int, shuffleId: Int, shuffleMergeId: Int): Boolean =
    blockTransferService.removeShuffleMerge(host, port, shuffleId, shuffleMergeId)

  override def close(): Unit = blockTransferService.close()

  override def shuffleMetrics(): MetricSet = blockTransferService.shuffleMetrics()

  override def diagnoseCorruption(
      host: String,
      port: Int,
      execId: String,
      shuffleId: Int,
      mapId: Long,
      reduceId: Int,
      checksum: Long,
      algorithm: String
  ): Cause =
    blockTransferService.diagnoseCorruption(host, port, execId, shuffleId, mapId, reduceId, checksum, algorithm)

  private val dfsPath = conf
    .getOption("spark.shuffle.dfs.path")
    .map(new Path(_))
    .getOrElse(
      throw new RuntimeException("DFS Shuffle Manager requires option spark.shuffle.dfs.path")
    )
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fileSystem = FileSystem.get(dfsPath.toUri, hadoopConf)

  private def getDfsPath(parts: String*): Path = {
    (Seq(appId, conf.get(APP_ATTEMPT_ID.key, "null")) ++ parts)
      .foldLeft(dfsPath) { case (dir, part) => new Path(dir, part) }
  }

  private case class BlockIdStateListener(delegate: BlockFetchingListener) extends BlockFetchingListener {
    val failedBlockIds: mutable.Buffer[String] = mutable.Buffer[String]()

    override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
      delegate.onBlockFetchSuccess(blockId, data)
    }

    override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
      logWarning(f"Failed to read block id $blockId", exception)
      failedBlockIds += blockId
    }
  }

  override def getHostLocalDirs(
      host: String,
      port: Int,
      execIds: Array[String],
      hostLocalDirsCompletable: CompletableFuture[java.util.Map[String, Array[String]]]
  ): Unit = {
    val thisExecId = SparkEnv.get.executorId
    if (execIds.length != 1 || execIds.exists(_ != thisExecId)) {
      hostLocalDirsCompletable.complete(new java.util.HashMap())
    } else {
      blockTransferService.getHostLocalDirs(host, port, execIds, hostLocalDirsCompletable)
    }
  }

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      tempFileManager: DownloadFileManager
  ): Unit = {
    // TODO: all shuffle blocks are read from dfs atm, make this configurable
    // TODO: implement detecting / memorizing dead executors
    val stateListener = BlockIdStateListener(listener)
    val executorIsAlive = false
    val pendingBlockIds = if (executorIsAlive) {
      try {
        logInfo(
          s"Fetching ${blockIds.length} blocks from executor $execId on $host:$port"
        )
        // blockTransferService.fetchBlocks calls listener.onBlockFetchFailure for failed blockIds,
        // intercept this via stateListener
        blockTransferService.fetchBlocks(host, port, execId, blockIds, stateListener, tempFileManager)
        stateListener.failedBlockIds.toArray
      } catch {
        case _: Exception =>
          // TODO: mark executor as dead
          stateListener.failedBlockIds.toArray
      }
    } else {
      blockIds
    }

    // fetch only the pending block ids from dfs
    if (pendingBlockIds.nonEmpty) {
      logInfo(s"Fetching ${pendingBlockIds.length} blocks from dfs")

      pendingBlockIds
        .map(BlockId.apply)
        .map(blockId => blockId -> read(blockId))
        .foreach {
          case (blockId, Success(buffer)) => write(blockId, buffer, Option(tempFileManager), listener)
          case (blockId, Failure(t))      => listener.onBlockFetchFailure(blockId.name, t)
        }
    }
  }

  private def read(blockId: BlockId): Try[ManagedBuffer] = {
    logInfo(f"Reading $blockId from $dfsPath")

    Try {
      val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
        case id: ShuffleBlockId =>
          (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
        case batchId: ShuffleBlockBatchId =>
          (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
        case _ =>
          throw SparkException.internalError(s"unexpected shuffle block id format: $blockId", category = "STORAGE")
      }

      val name = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
      val hash = JavaUtils.nonNegativeHash(name)
      val indexFile = getDfsPath(shuffleId.toString, hash.toString, name)
      logInfo(s"Reading index file $indexFile")
      val start = startReduceId * 8L
      val end = endReduceId * 8L
      Utils.tryWithResource(fileSystem.open(indexFile)) { inputStream =>
        Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
          index.skip(start)
          val offset = index.readLong()
          index.skip(end - (start + 8L))
          val nextOffset = index.readLong()
          val name = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
          val hash = JavaUtils.nonNegativeHash(name)
          val dataFile = getDfsPath(shuffleId.toString, hash.toString, name)
          logInfo(s"Reading data file $dataFile")
          val size = nextOffset - offset
          logDebug(s"To byte array $size")
          val array = new Array[Byte](size.toInt)
          val startTimeNs = System.nanoTime()
          Utils.tryWithResource(fileSystem.open(dataFile)) { f =>
            f.seek(offset)
            f.readFully(array)
            logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
          }
          new NioManagedBuffer(ByteBuffer.wrap(array))
        }
      }
    }
  }

  private def write(
      blockId: BlockId,
      buffer: ManagedBuffer,
      fileManager: Option[DownloadFileManager],
      listener: BlockFetchingListener
  ): Unit = {
    if (fileManager.isDefined) {
      val file = fileManager.get.createTempFile(transportConf)
      val channel = file.openForWriting()
      channel.write(buffer.nioByteBuffer())
      listener.onBlockFetchSuccess(blockId.name, channel.closeAndRead())
      if (!fileManager.get.registerTempFileToClean(file)) {
        file.delete()
      }
    } else {
      listener.onBlockFetchSuccess(blockId.name, buffer)
    }
  }

}
