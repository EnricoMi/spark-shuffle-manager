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
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}

import java.util.Collections

class DfsShuffleManager(val conf: SparkConf) extends ShuffleManager with Logging {
  logInfo("DfsShuffleManager created")
  val base = new SortShuffleManager(conf)
  lazy val resolver = new IndexShuffleBlockResolver(conf, SparkEnv.get.blockManager, Collections.emptyMap())

  override val shuffleBlockResolver = new DfsShuffleBlockResolver(base.shuffleBlockResolver)

  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    logInfo("registering shuffle id " + shuffleId)
    val base = this.base.registerShuffle(shuffleId, dependency)
    new DfsShuffleHandle(shuffleId, dependency.partitioner, base)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Long, context: TaskContext, metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    logInfo("creating writer for shuffle " + handle)
    val base = this.base.getWriter[K, V](handle.asInstanceOf[DfsShuffleHandle].base, mapId, context, metrics)
    new DfsShuffleWriter[K, V](handle.asInstanceOf[DfsShuffleHandle], base, mapId, this)
  }

  override def getReader[K, C](handle: ShuffleHandle, startMapIndex: Int, endMapIndex: Int, startPartition: Int, endPartition: Int, context: TaskContext, metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    logInfo("creating writer for shuffle " + handle)
    val base = this.base.getReader[K, C](handle.asInstanceOf[DfsShuffleHandle].base, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
    new DfsShuffleReader[K, C](handle.asInstanceOf[DfsShuffleHandle], base)
  }

  def sync(handle: DfsShuffleHandle, mapId: Long): Unit = {
    logInfo("syncing " + resolver.getDataFile(handle.shuffleId, mapId))
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo("unregistering shuffle id " + shuffleId)
    base.unregisterShuffle(shuffleId) && true
  }

  override def stop(): Unit = {
    logInfo("stopping manager")
    shuffleBlockResolver.stop()
    base.stop()
  }
}
