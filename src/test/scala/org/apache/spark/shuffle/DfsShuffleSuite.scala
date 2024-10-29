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

import org.apache.spark.executor.{ShuffleWriteMetrics, TempShuffleReadMetrics}
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.{ConstantPartitioner, ShuffleDependency, SparkConf, TaskContext}
import org.scalatest.funsuite.AnyFunSuite
import uk.co.gresearch.spark.SparkTestSession

class DfsShuffleSuite extends AnyFunSuite with SparkTestSession {
  test("ShuffleManager construction") {
    val manager = new DfsShuffleManager(new SparkConf())
    val dependency = new ShuffleDependency[Int, String, Any](
      new EmptyRDD[Product2[Int, String]](spark.sparkContext),
      new ConstantPartitioner
    )
    val context = TaskContext.get()
    val readMetrics = new TempShuffleReadMetrics()
    val writeMetrics = new ShuffleWriteMetrics()

    val handle = manager.registerShuffle(0, dependency)
    manager.getWriter(handle, 0, context, writeMetrics)
    manager.getReader(handle, 0, Int.MaxValue, 0, Int.MaxValue, context, readMetrics)
    manager.unregisterShuffle(0)
    val resolver = manager.shuffleBlockResolver
    resolver.stop()
    manager.stop()
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
