/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.test

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.util.Utils

/**
 * Base class for tests with SparkSQL VectorUDT data.
 */
abstract class VectorQueryTest extends QueryTest with TestHiveSingleton {
  import hiveContext.implicits._

  private var trainDir: File = _
  private var testDir: File = _

   // A `libsvm` schema is (Double, ml.linalg.Vector)
  protected var mllibTrainDf: DataFrame = _
  protected var mllibTestDf: DataFrame  = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val trainLines =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0 2:4.0 4:5.0 6:6.0
        |1 1:1.1 4:1.0 5:2.3 7:1.0
        |1 1:1.0 4:1.5 5:2.1 7:1.2
      """.stripMargin
    trainDir = Utils.createTempDir()
    Files.write(trainLines, new File(trainDir, "train-00000"), StandardCharsets.UTF_8)
    val testLines =
      """
        |1 1:1.3 3:2.1 5:2.8
        |0 2:3.9 4:5.3 6:8.0
      """.stripMargin
    testDir = Utils.createTempDir()
    Files.write(testLines, new File(testDir, "test-00000"), StandardCharsets.UTF_8)

    mllibTrainDf = spark.read.format("libsvm").load(trainDir.getAbsolutePath)
    // Must be cached because rowid() is deterministic
    mllibTestDf = spark.read.format("libsvm").load(testDir.getAbsolutePath)
      .withColumn("rowid", rowid()).cache
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(trainDir)
      Utils.deleteRecursively(testDir)
    } finally {
      super.afterAll()
    }
  }

  protected def withTempModelDir(f: String => Unit): Unit = {
    var tempDir: File = null
    try {
      tempDir = Utils.createTempDir()
      f(tempDir.getAbsolutePath + "/xgboost_models")
    } catch {
      case e: Throwable => fail(s"Unexpected exception detected: ${e}")
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
