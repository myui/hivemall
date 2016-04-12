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

import org.apache.spark.ml.feature.HmLabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.apache.spark.streaming.HivemallStreamingOps._

class HivemallOpsSuite extends TestSuiteBase {

  import org.apache.spark.sql.hive.test.TestHive.implicits._
  import org.apache.spark.streaming.HivemallOpsSuite._

  test("streaming") {
    withStreamingContext(new StreamingContext(TestHive.sparkContext, Milliseconds(500))) { ssc =>
      val input = Seq(
        Seq(HmLabeledPoint(features = "1:0.6" :: "2:0.1" :: Nil)),
        Seq(HmLabeledPoint(features = "2:0.9" :: Nil)),
        Seq(HmLabeledPoint(features = "1:0.2" :: Nil)),
        Seq(HmLabeledPoint(features = "2:0.1" :: Nil)),
        Seq(HmLabeledPoint(features = "0:0.6" :: "2:0.4" :: Nil))
      )

      val stream = new TestInputStream[HmLabeledPoint](ssc, input, 2)

      // Apply predictions on input streams
      val prediction = stream.predict { testDf =>
          val testDf_exploded = testDf
            .explode_array($"features")
            .select(rowid(), extract_feature($"feature"), extract_weight($"feature"))
          val predictDf = testDf_exploded
            .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
            .select($"rowid", ($"weight" * $"value").as("value"))
            .groupby("rowid").sum("value")
            .select($"rowid", sigmoid($"SUM(value)"))
          assert(predictDf.count > 0)
          predictDf
        }

      // Dummy output stream
      prediction.foreachRDD { _ => {}}
    }
  }
}

object HivemallOpsSuite {
  implicit val sqlContext = TestHive
  val model =
     TestHive.createDataFrame(
       TestHive.sparkContext.parallelize(
         Row(0, 0.3f) ::
         Row(1, 0.1f) ::
         Row(2, 0.6f) ::
         Row(3, 0.2f) ::
         Nil
       ),
       StructType(
         StructField("feature", IntegerType, true) ::
         StructField("weight", FloatType, true) ::
         Nil)
     )
}

