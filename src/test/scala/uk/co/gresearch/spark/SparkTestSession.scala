/*
 * Copyright 2020 G-Research
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

package uk.co.gresearch.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkTestSession {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[16]")
      .appName("Spark Tests")
      .config("spark.sql.shuffle.partitions", 24)
      .config("spark.sql.adaptive.enabled", false)
      .config("spark.local.dir", ".")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.BackupShuffleManager")
      .config("spark.shuffle.backup.path", "/tmp/spark-backup-shuffle-service")
      .getOrCreate()
  }

  lazy val sc: SparkContext = spark.sparkContext

  lazy val sql: SQLContext = spark.sqlContext

}
