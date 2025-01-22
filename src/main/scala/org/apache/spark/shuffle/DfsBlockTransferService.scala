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

package org.apache.spark.shuffle

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.APP_ATTEMPT_ID
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockBatchId, ShuffleBlockId, ShuffleDataBlockId, ShuffleIndexBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf, SparkException}

import java.io.DataInputStream
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class DfsBlockTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    serializerManager: SerializerManager,
    bindAddress: String,
    hostName: String,
    port: Int,
    numCores: Int,
    driverEndPointRef: RpcEndpointRef = null
) extends NettyBlockTransferService(
      conf,
      securityManager,
      serializerManager,
      bindAddress,
      hostName,
      port,
      numCores,
      driverEndPointRef
    )
    with Logging {

  var blockManager: Option[BlockManager] = None

  override def init(blockDataManager: BlockDataManager): Unit = {
    super.init(blockDataManager)
    blockManager = Some(blockDataManager.asInstanceOf[BlockManager])
  }

  private val dfsPath = conf
    .getOption("spark.shuffle.dfs.path")
    .map(new Path(_))
    .getOrElse(
      throw new RuntimeException("DFS Shuffle Manager requires option spark.shuffle.dfs.path")
    )
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fileSystem = FileSystem.get(dfsPath.toUri, hadoopConf)

  private def getDfsPath(sub: String): Path =
    Seq(appId, conf.get(APP_ATTEMPT_ID.key, "null"), sub)
      .foldLeft(dfsPath) { case (dir, part) => new Path(dir, part) }

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
      hostLocalDirsCompletable: CompletableFuture[util.Map[String, Array[String]]]
  ): Unit = {
    val thisExecId = blockManager.get.executorId
    if (execIds.length != 1 || execIds.exists(_ != thisExecId)) {
      hostLocalDirsCompletable.complete(new util.HashMap())
    } else {
      super.getHostLocalDirs(host, port, execIds, hostLocalDirsCompletable)
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
        // super.fetchBlocks calls listener.onBlockFetchFailure for failed blockIds,
        // intercept this via stateListener
        super.fetchBlocks(host, port, execId, blockIds, stateListener, tempFileManager)
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
    val subId = blockId.name.split("_")(1)
    val dfsPath = getDfsPath(subId)
    logInfo(f"Reading $blockId from dfs: $dfsPath")

    Try {
      val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
        case id: ShuffleBlockId =>
          (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
        case batchId: ShuffleBlockBatchId =>
          (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
        case _ =>
          throw SparkException.internalError(
            s"unexpected shuffle block id format: $blockId", category = "STORAGE")
      }

      val name = ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
      val hash = JavaUtils.nonNegativeHash(name)
      val indexFile = new Path(dfsPath, s"$appId/$shuffleId/$hash/$name")
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
          val dataFile = new Path(dfsPath, s"$appId/$shuffleId/$hash/$name")
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
