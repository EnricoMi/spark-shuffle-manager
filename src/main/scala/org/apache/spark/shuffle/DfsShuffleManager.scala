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
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.ThreadUtils
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}

import java.io.File
import java.nio.file.{CopyOption, Files, Path, StandardCopyOption}
import java.util.Collections
import java.util.concurrent.{Future, TimeUnit}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.reflect.io.Directory
import scala.util.Try

class DfsShuffleManager(val conf: SparkConf) extends ShuffleManager with Logging {
  logInfo("DfsShuffleManager created")
  val base = new SortShuffleManager(conf)
  lazy val resolver = new IndexShuffleBlockResolver(conf, SparkEnv.get.blockManager, Collections.emptyMap())

  private val dfsPath = conf
    .getOption("spark.shuffle.dfs.path")
    .map(new File(_))
    .getOrElse(
      throw new RuntimeException("DFS Shuffle Manager requires option spark.shuffle.dfs.path")
    )

  private val syncThreadPool =
    ThreadUtils.newDaemonCachedThreadPool("dfs-shuffle-manager-sync-thread-pool", 16)
  private implicit val syncExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(syncThreadPool)
  private val syncTasks: mutable.Buffer[Future[_]] = mutable.Buffer()

  override val shuffleBlockResolver = new DfsShuffleBlockResolver(base.shuffleBlockResolver)

  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    logInfo("registering shuffle id " + shuffleId)
    val base = this.base.registerShuffle(shuffleId, dependency)
    new DfsShuffleHandle(shuffleId, dependency.partitioner, base)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter
  ): ShuffleWriter[K, V] = {
    logInfo("creating writer for shuffle " + handle)
    val base = this.base.getWriter[K, V](handle.asInstanceOf[DfsShuffleHandle].base, mapId, context, metrics)
    new DfsShuffleWriter[K, V](handle.asInstanceOf[DfsShuffleHandle], base, mapId, this)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter
  ): ShuffleReader[K, C] = {
    logInfo("creating writer for shuffle " + handle)
    val base = this.base.getReader[K, C](
      handle.asInstanceOf[DfsShuffleHandle].base,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics
    )
    new DfsShuffleReader[K, C](handle.asInstanceOf[DfsShuffleHandle], base)
  }

  private def getDestination(shuffleId: Int, parts: String*): Path = {
    (Seq(conf.getAppId, conf.get(APP_ATTEMPT_ID.key, "null"), shuffleId.toString) ++ parts)
      .foldLeft(dfsPath) { case (dir, part) => new File(dir, part) }
      .toPath
  }

  def sync(handle: DfsShuffleHandle, mapId: Long): Unit = {
    val dataFile = resolver.getDataFile(handle.shuffleId, mapId).toPath
    val indexFile = resolver.getIndexFile(handle.shuffleId, mapId).toPath
    Seq(dataFile, indexFile)
      .map(path => SyncTask(path, getDestination(handle.shuffleId, path.getParent.toFile.getName, path.toFile.getName)))
      .map(syncExecutionContext.submit)
      .foreach(syncTasks.addOne)
  }

  private def removeDir(path: Path): Boolean = {
    logInfo(f"removing $path")
    Try(() => new Directory(path.toFile).deleteRecursively()).recover { case t: Throwable =>
      logWarning(f"Failed to delete directory $path", t)
    }.isSuccess
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo("unregistering shuffle id " + shuffleId)
    val removed = removeDir(getDestination(shuffleId))
    val unregistered = base.unregisterShuffle(shuffleId)
    removed && unregistered
  }

  override def stop(): Unit = {
    logInfo("stopping manager")

    // wait or sync tasks to finish
    syncThreadPool.shutdown()
    syncTasks
      .map(task => Try(() => task.get()))
      .filter(_.isFailure)
      .map(_.failed.get)
      .foreach(logWarning("copying file failed", _))
    syncExecutionContext.awaitTermination(1, TimeUnit.SECONDS)

    // stop underlying instances
    shuffleBlockResolver.stop()
    base.stop()
  }
}

case class SyncTask(source: Path, destination: Path) extends Runnable with Logging {
  override def run(): Unit = {
    logInfo(s"copying $source to $destination")
    destination.getParent.toFile.mkdirs()
    Files.copy(source, destination, StandardCopyOption.COPY_ATTRIBUTES)
  }
}
