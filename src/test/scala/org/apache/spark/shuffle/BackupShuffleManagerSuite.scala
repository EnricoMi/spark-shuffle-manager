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
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.network.BackupBlockTransferService
import org.apache.spark.util.Clock
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession

import java.io.FileNotFoundException
import java.nio.file.Files

class BackupShuffleManagerSuite extends AnyFunSuite with SparkTestSession {
  Seq(0, 1000, 3000, 6000).foreach { replicationMs =>
    test(s"Consider replication delay - ${replicationMs}ms") {
      val dir = Files.createTempDirectory("BackupShuffleManagerSuite")
      val path = new Path(dir.toFile.getAbsolutePath)
      val file = new Path(path, "file")
      val delay = 5000 // max allowed replication
      val wait = 2000 // time between open file attempts
      val conf = new SparkConf(false)
        .set(BackupBlockTransferService.BACKUP_REPLICATION_DELAY.key, s"${delay}ms")
        .set(BackupBlockTransferService.BACKUP_REPLICATION_WAIT.key, s"${wait}ms")
      val reader = BackupBlockTransferService.RetryingReader(conf)
      val filesystem = FileSystem.get(SparkHadoopUtil.get.newConfiguration(conf))
      val startMs = 123000000L * 1000L // arbitrary system time
      val clock = new DelayedActionClock(replicationMs, startMs)(filesystem.create(file).close())

      try {
        if (replicationMs <= delay) {
          // expect open to succeed
          val in = reader.open(filesystem, file, clock)
          assert(in != null)

          // how many waits are expected to observe replication
          val expectedWaits = Math.ceil(replicationMs.toFloat / wait).toInt
          assert(clock.timeMs == startMs + expectedWaits * wait)
          assert(clock.waited == expectedWaits)
          in.close()
        } else {
          // expect open to fail
          assertThrows[FileNotFoundException](reader.open(filesystem, file, clock))

          // how many waits are expected to observe delay
          val expectedWaits = delay / wait
          assert(clock.timeMs == startMs + expectedWaits * wait)
          assert(clock.waited == expectedWaits)
        }
      } finally {
        filesystem.delete(path, true)
      }
    }
  }

  test("dataset") {
    import spark.implicits._
    spark
      .range(1, 100, 1, 10)
      .groupBy($"id" % 7)
      .count()
      .groupBy($"count")
      .count()
      .show(false)
    spark.range(1, 100).show()
  }
}

class DelayedActionClock(delayMs: Long, startTimeMs: Long)(action: => Unit) extends Clock {
  var timeMs: Long = startTimeMs
  var waited: Int = 0
  var triggered: Boolean = false

  if (delayMs == 0) trigger()

  private def trigger(): Unit = {
    if (!triggered) {
      triggered = true
      action
    }
  }

  override def getTimeMillis(): Long = timeMs
  override def nanoTime(): Long = timeMs * 1000000
  override def waitTillTime(targetTime: Long): Long = {
    waited += 1
    if (targetTime >= startTimeMs + delayMs) {
      timeMs = startTimeMs + delayMs
      trigger()
    }
    timeMs = targetTime
    targetTime
  }
}
