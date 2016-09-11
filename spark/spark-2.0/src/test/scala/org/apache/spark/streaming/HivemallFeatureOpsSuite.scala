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

package org.apache.spark.streaming

import reflect.ClassTag

import org.apache.spark.ml.feature.HivemallLabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.streaming.HivemallStreamingOps._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.test.HivemallFeatureQueryTest

/**
 * This is an input stream just for tests.
 */
private[this] class TestInputStream[T: ClassTag](
    ssc: StreamingContext,
    input: Seq[Seq[T]],
    numPartitions: Int) extends InputDStream[T](ssc) {

  override def start() {}

  override def stop() {}

  override def compute(validTime: Time): Option[RDD[T]] = {
    logInfo("Computing RDD for time " + validTime)
    val index = ((validTime - zeroTime) / slideDuration - 1).toInt
    val selectedInput = if (index < input.size) input(index) else Seq[T]()

    // lets us test cases where RDDs are not created
    if (selectedInput == null) {
      return None
    }

    // Report the input data's information to InputInfoTracker for testing
    val inputInfo = StreamInputInfo(id, selectedInput.length.toLong)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    val rdd = ssc.sc.makeRDD(selectedInput, numPartitions)
    logInfo("Created RDD " + rdd.id + " with " + selectedInput)
    Some(rdd)
  }
}

final class HivemallFeatureOpsSuite extends HivemallFeatureQueryTest {

  // This implicit value used in `HivemallStreamingOps`
  implicit val sqlCtx = hiveContext

  /**
   * Run a block of code with the given StreamingContext.
   * This method do not stop a given SparkContext because other tests share the context.
   */
  private def withStreamingContext[R](ssc: StreamingContext)(block: StreamingContext => R): Unit = {
    try {
      block(ssc)
      ssc.start()
      ssc.awaitTerminationOrTimeout(10 * 1000) // 10s wait
    } finally {
      try {
        ssc.stop(stopSparkContext = false)
      } catch {
        case e: Exception => logError("Error stopping StreamingContext", e)
      }
    }
  }

  test("streaming") {
    import sqlCtx.implicits._

    // We assume we build a model in advance
    val testModel = Seq(
      ("0", 0.3f), ("1", 0.1f), ("2", 0.6f), ("3", 0.2f)
    ).toDF("feature", "weight")

    withStreamingContext(new StreamingContext(sqlCtx.sparkContext, Milliseconds(100))) { ssc =>
      val inputData = Seq(
        Seq(HivemallLabeledPoint(features = "1:0.6" :: "2:0.1" :: Nil)),
        Seq(HivemallLabeledPoint(features = "2:0.9" :: Nil)),
        Seq(HivemallLabeledPoint(features = "1:0.2" :: Nil)),
        Seq(HivemallLabeledPoint(features = "2:0.1" :: Nil)),
        Seq(HivemallLabeledPoint(features = "0:0.6" :: "2:0.4" :: Nil))
      )

      val inputStream = new TestInputStream[HivemallLabeledPoint](ssc, inputData, 1)

      // Apply predictions on input streams
      val prediction = inputStream.predict { streamDf =>
          val df = streamDf.select(rowid(), $"features").explode_array($"features")
          val testDf = df.select(
            // TODO: `$"feature"` throws AnalysisException, why?
            $"rowid", extract_feature(df("feature")), extract_weight(df("feature"))
          )
          testDf.join(testModel, testDf("feature") === testModel("feature"), "LEFT_OUTER")
            .select($"rowid", ($"weight" * $"value").as("value"))
            .groupby("rowid").sum("value")
            .as("rowid", "value")
            .select($"rowid", sigmoid($"value"))
        }

      // Dummy output stream
      prediction.foreachRDD(_ => {})
    }
  }
}
