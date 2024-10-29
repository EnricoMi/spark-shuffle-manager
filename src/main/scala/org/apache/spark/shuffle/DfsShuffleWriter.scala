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
import org.apache.spark.scheduler.MapStatus

import java.util.concurrent.Future
import scala.collection.mutable

class DfsShuffleWriter[K, V](
    val handle: DfsShuffleHandle,
    base: ShuffleWriter[K, V],
    mapId: Long,
    manager: DfsShuffleManager
) extends ShuffleWriter[K, V]
    with Logging {
  private val futures: mutable.Buffer[Future[_]] = mutable.Buffer()

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    base.write(records)
    futures.addAll(manager.sync(handle, mapId))
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    logInfo("stopping writer with success=" + success)

    if (success) {
      // wait for all sync tasks to complete
      futures.foreach(_.get())
    } else {
      // cancel all sync tasks
      futures.foreach(_.cancel(true))
    }

    base.stop(success)
  }

  override def getPartitionLengths(): Array[Long] = {
    base.getPartitionLengths()
  }
}
