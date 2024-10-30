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

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      tempFileManager: DownloadFileManager
  ): Unit = {
    logInfo(
      s"Fetching ${blockIds.length} blocks from executor $execId on $host:$port: ${blockIds.slice(0, 10).mkString(", ")}"
    )
    val executorIsAlive = blockIds.exists(!BlockId.apply(_).isShuffle)
    if (executorIsAlive) {
      try {
        // TODO: super.fetchBlocks will call listener.onBlockFetchFailure for failed blockIds
        //       these calls need to be intercepted, only those blockIds need to be fetched below
        super.fetchBlocks(host, port, execId, blockIds, listener, tempFileManager)
        return
      } catch {
        case _: Exception => // mark executor as dead
      }
    }

    logInfo(
      s"Fetching ${blockIds.length} blocks from dfs: ${blockIds.slice(0, 10).mkString(", ")}"
    )
    // BlockId.apply(blockId)
    // blockmanager.getLocalBlockData uses shuffleManager.shuffleBlockResolver.getBlockData(blockId)
    // IndexShuffleBlockResolver reads from local files
    // instantitate BlockManager with IndexShuffleBlockResolver configured with locally mounted dfs path and use blockmanager.getLocalBlockData(blockid) as fallback
    // val envBlockManager = SparkEnv.get.blockManager
    // val blockManager = new BlockManager(envBlockManager.executorId, null, envBlockManager.master)
    // val resolver = new IndexShuffleBlockResolver(conf, blockManager, java.util.Map.ofEntries())
    // val blockResolver = new IndexShuffleBlockResolver(conf, blockManager, Collections.emptyMap)

    blockIds
      .map(BlockId.apply)
      .map(blockId => blockId -> read(blockId))
      .foreach {
        case (blockId, Success(buffer)) => write(blockId, buffer, Option(tempFileManager), listener)
        case (blockId, Failure(t))      => listener.onBlockFetchFailure(blockId.name, t)
      }
  }

  private def read(blockId: BlockId): Try[ManagedBuffer] = {
    val subId = blockId.name.split("_")(1)
    val dfsPath = getDfsPath(subId)
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
