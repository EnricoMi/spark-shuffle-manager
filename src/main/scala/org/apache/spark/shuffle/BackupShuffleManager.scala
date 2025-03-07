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
import org.apache.spark.internal.config.{APP_ATTEMPT_ID, EXECUTOR_ID}
import org.apache.spark.network.BackupBlockTransferService
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager.canUseBatchFetch
import org.apache.spark.storage.BackupBlockManager
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.{ShuffleDependency, SparkConf, SparkContext, SparkEnv, TaskContext}

import java.util.concurrent.{Future, TimeUnit}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.Try

class BackupShuffleManager(val conf: SparkConf) extends SortShuffleManager(conf) with Logging {
  logInfo("BackupShuffleManager created")

  private lazy val appId = conf.getAppId
  private val backupPath = conf
    .getOption("spark.shuffle.backup.path")
    .map(new Path(_))
    .getOrElse(
      throw new RuntimeException("BackupShuffleManager requires option spark.shuffle.backup.path")
    )
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  private val fileSystem = FileSystem.get(backupPath.toUri, hadoopConf)
  private val isDriver = conf.get(EXECUTOR_ID).exists(_.equals(SparkContext.DRIVER_IDENTIFIER))

  // fail-fast on driver if backup path cannot be accessed / written to
  if (isDriver) {
    try {
      if (!fileSystem.exists(backupPath)) {
        fileSystem.mkdirs(backupPath)
      }
    } catch {
      case e: Exception => throw new RuntimeException(s"Cannot access backup path $backupPath", e)
    }
  }

  private val threadPool =
    ThreadUtils.newDaemonCachedThreadPool("backup-shuffle-manager-thread-pool", 16)
  private implicit val executionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(threadPool)
  private val tasks: mutable.Buffer[Future[_]] = mutable.Buffer()

  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    logInfo("registering shuffle id " + shuffleId)
    val handle = super.registerShuffle(shuffleId, dependency)
    new BackupShuffleHandle(shuffleId, dependency.partitioner, handle)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter
  ): ShuffleWriter[K, V] = {
    logInfo("creating writer for shuffle " + handle)
    val writer = super.getWriter[K, V](handle.asInstanceOf[BackupShuffleHandle].handle, mapId, context, metrics)
    new BackupShuffleWriter[K, V](handle.asInstanceOf[BackupShuffleHandle], writer, mapId, this)
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
    logInfo("creating reader for shuffle " + handle)
    val baseShuffleHandle = handle.asInstanceOf[BackupShuffleHandle].handle.asInstanceOf[BaseShuffleHandle[K, _, C]]
    val (blocksByAddress, canEnableBatchFetch) =
      if (baseShuffleHandle.dependency.isShuffleMergeFinalizedMarked) {
        val res = SparkEnv.get.mapOutputTracker.getPushBasedShuffleMapSizesByExecutorId(
          handle.shuffleId,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition
        )
        (res.iter, res.enableBatchFetch)
      } else {
        val address = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
          handle.shuffleId,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition
        )
        (address, true)
      }

    val backupBlockTransferService =
      new BackupBlockTransferService(conf, SparkEnv.get.blockManager.blockTransferService)
    val backupBlockManager = new BackupBlockManager(SparkEnv.get.blockManager, backupBlockTransferService)
    assert(backupBlockManager.blockTransferService.isInstanceOf[BackupBlockTransferService])
    assert(backupBlockManager.blockStoreClient.isInstanceOf[BackupBlockTransferService])
    new BlockStoreShuffleReader(
      baseShuffleHandle,
      blocksByAddress,
      context,
      metrics,
      blockManager = backupBlockManager,
      shouldBatchFetch = canEnableBatchFetch && canUseBatchFetch(startPartition, endPartition, context)
    )
  }

  private[spark] def getDestination(shuffleId: Int, parts: String*): Path = {
    val hash = if (parts.nonEmpty) Seq(JavaUtils.nonNegativeHash(parts.last).toString) else Seq.empty
    (Seq(appId, conf.get(APP_ATTEMPT_ID.key, "null"), shuffleId.toString) ++ hash ++ parts)
      .foldLeft(backupPath) { case (dir, part) => new Path(dir, part) }
  }

  def sync(handle: BackupShuffleHandle, mapId: Long): Unit = {
    val shuffleId = handle.shuffleId
    val dataFile = shuffleBlockResolver.getDataFile(handle.shuffleId, mapId)
    val indexFile = shuffleBlockResolver.getIndexFile(handle.shuffleId, mapId)

    tasks.synchronized {
      if (indexFile.exists()) {
        Seq(dataFile, indexFile)
          .filter(_.exists())
          .map(path => new Path(Utils.resolveURI(path.getAbsolutePath)))
          .map(path => SyncTask(path, getDestination(shuffleId, path.getName), fileSystem))
          .map(executionContext.submit)
          .foreach(tasks += (_))
      }
    }
  }

  private def removeDir(path: Path): Unit = {
    tasks.synchronized {
      tasks += executionContext.submit(RemoveDirTask(path, fileSystem))
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    logInfo("unregistering shuffle id " + shuffleId)
    if (isDriver) removeDir(getDestination(shuffleId))
    super.unregisterShuffle(shuffleId)
  }

  override def stop(): Unit = {
    logInfo("stopping manager")

    // stop underlying manager
    super.stop()

    // remove all shuffle data
    if (isDriver) {
      removeDir(new Path(backupPath, appId))
    }

    // wait or sync tasks to finish
    threadPool.shutdown()
    tasks.synchronized {
      tasks
        .map(task => Try(() => task.get()))
        .filter(_.isFailure)
        .map(_.failed.get)
        .foreach(logWarning("copying file failed", _))
    }
    executionContext.awaitTermination(1, TimeUnit.SECONDS)

    logInfo("stopped manager")
  }
}

case class SyncTask(source: Path, destination: Path, fileSystem: FileSystem) extends Runnable with Logging {
  override def run(): Unit = {
    logInfo(s"copying $source to $destination")
    fileSystem.mkdirs(destination.getParent)
    fileSystem.copyFromLocalFile(source, destination)
    logInfo(s"copied $source to $destination")
  }
}

case class RemoveDirTask(path: Path, fileSystem: FileSystem) extends Runnable with Logging {
  override def run(): Unit = {
    logInfo(f"removing $path")
    Try {
      if (fileSystem.delete(path, true)) {
        logInfo(s"removed $path")
      } else {
        logWarning(s"removing $path incomplete")
      }
    }.recover { case t: Throwable =>
      logWarning(f"removing $path failed", t)
    }
  }
}