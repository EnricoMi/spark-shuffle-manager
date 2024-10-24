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

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.serializer.SerializerManager

class BlockTransferService(
    conf: SparkConf,
    securityManager: SecurityManager,
    serializerManager: SerializerManager,
    bindAddress: String,
    hostName: String,
    port: Int,
    numCores: Int,
    driverEndPointRef: RpcEndpointRef = null) extends NettyBlockTransferService(
  conf, securityManager, serializerManager, bindAddress, hostName, port, numCores,
  driverEndPointRef) {

  override def fetchBlocks(
      host: String,
      port: Int,
      execId: String,
      blockIds: Array[String],
      listener: BlockFetchingListener,
      tempFileManager: DownloadFileManager): Unit =
    super.fetchBlocks(host, port, execId, blockIds, listener, tempFileManager)
}
