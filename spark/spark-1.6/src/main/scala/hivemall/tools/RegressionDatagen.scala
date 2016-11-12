/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.tools

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.types._

object RegressionDatagen {

  /**
   * Generate data for regression/classification.
   * See [[hivemall.dataset.LogisticRegressionDataGeneratorUDTF]]
   * for the details of arguments below.
   */
  def exec(sc: SQLContext,
           n_partitions: Int = 2,
           min_examples: Int = 1000,
           n_features: Int = 10,
           n_dims: Int = 200,
           seed: Int = 43,
           dense: Boolean = false,
           prob_one: Float = 0.6f,
           sort: Boolean = false,
           cl: Boolean = false): DataFrame = {

    require(n_partitions > 0, "Non-negative #n_partitions required.")
    require(min_examples > 0, "Non-negative #min_examples required.")
    require(n_features > 0, "Non-negative #n_features required.")
    require(n_dims > 0, "Non-negative #n_dims required.")

    // Calculate #examples to generate in each partition
    val n_examples = (min_examples + n_partitions - 1) / n_partitions

    val df = sc.createDataFrame(
        sc.sparkContext.parallelize((0 until n_partitions).map(Row(_)), n_partitions),
        StructType(
          StructField("data", IntegerType, true) ::
          Nil)
      )
    import sc.implicits._
    df.lr_datagen(
      s"-n_examples $n_examples -n_features $n_features -n_dims $n_dims -prob_one $prob_one"
        + (if (dense) " -dense" else "")
        + (if (sort) " -sort" else "")
        + (if (cl) " -cl" else ""))
      .select($"label".cast(DoubleType).as("label"), $"features")
  }
}
