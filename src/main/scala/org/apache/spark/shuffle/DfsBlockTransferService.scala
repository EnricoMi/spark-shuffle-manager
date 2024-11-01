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

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.APP_ATTEMPT_ID
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, DownloadFileWritableChannel}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.BlockId
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

import java.io.File
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

  val dfsPath: File = conf
    .getOption("spark.shuffle.dfs.path")
    .map(new File(_))
    .getOrElse(
      throw new RuntimeException("DFS Shuffle Manager requires option spark.shuffle.dfs.path")
    )

  private def getDfsPath(sub: String): String =
    Seq(conf.getAppId, conf.get(APP_ATTEMPT_ID.key, "null"), sub)
      .foldLeft(dfsPath) { case (dir, part) => new File(dir, part) }
      .getPath

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
    val executorIsAlive = blockIds.exists(!BlockId.apply(_).isShuffle)
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
    Try(SparkEnv.get.blockManager.getHostLocalShuffleData(blockId, Array(dfsPath)))
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
