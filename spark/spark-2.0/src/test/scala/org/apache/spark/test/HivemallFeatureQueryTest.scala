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

import scala.collection.mutable.Seq

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.QueryTest

import hivemall.tools.RegressionDatagen

/**
 * Base class for tests with Hivemall features.
 */
abstract class HivemallFeatureQueryTest extends QueryTest with TestHiveSingleton {

  import hiveContext.implicits._

  protected val DummyInputData =
    Seq(
      (0, 0), (1, 1), (2, 2), (3, 3)
    ).toDF("c0", "c1")

  protected val IntList2Data =
    Seq(
      (8 :: 5 :: Nil, 6 :: 4 :: Nil),
      (3 :: 1 :: Nil, 3 :: 2 :: Nil),
      (2 :: Nil, 3 :: Nil)
    ).toDF("target", "predict")

  protected val Float2Data =
    Seq(
      (0.8f, 0.3f), (0.3f, 0.9f), (0.2f, 0.4f)
    ).toDF("target", "predict")

  protected val TinyTrainData =
    Seq(
      (0.0, "1:0.8" :: "2:0.2" :: Nil),
      (1.0, "2:0.7" :: Nil),
      (0.0, "1:0.9" :: Nil)
    ).toDF("label", "features")

  protected val TinyTestData =
    Seq(
      (0.0, "1:0.6" :: "2:0.1" :: Nil),
      (1.0, "2:0.9" :: Nil),
      (0.0, "1:0.2" :: Nil),
      (0.0, "2:0.1" :: Nil),
      (0.0, "0:0.6" :: "2:0.4" :: Nil)
    ).toDF("label", "features")

  protected val LargeRegrTrainData = RegressionDatagen.exec(
      hiveContext,
      n_partitions = 2,
      min_examples = 100000,
      seed = 3,
      prob_one = 0.8f
    ).cache

  protected val LargeRegrTestData = RegressionDatagen.exec(
      hiveContext,
      n_partitions = 2,
      min_examples = 100,
      seed = 3,
      prob_one = 0.5f
    ).cache

  protected val LargeClassifierTrainData = RegressionDatagen.exec(
      hiveContext,
      n_partitions = 2,
      min_examples = 100000,
      seed = 5,
      prob_one = 0.8f,
      cl = true
    ).cache

  protected val LargeClassifierTestData = RegressionDatagen.exec(
      hiveContext,
      n_partitions = 2,
      min_examples = 100,
      seed = 5,
      prob_one = 0.5f,
      cl = true
    ).cache
}
