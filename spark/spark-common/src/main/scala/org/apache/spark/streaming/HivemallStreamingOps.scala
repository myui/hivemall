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

import scala.reflect.ClassTag

import org.apache.spark.ml.feature.HivemallLabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.DStream

final class HivemallStreamingOps(ds: DStream[HivemallLabeledPoint]) {

  def predict[U: ClassTag](f: DataFrame => DataFrame)(implicit sqlContext: SQLContext)
      : DStream[Row] = {
    ds.transform[Row] { rdd: RDD[HivemallLabeledPoint] =>
      f(sqlContext.createDataFrame(rdd)).rdd
    }
  }
}

object HivemallStreamingOps {

  /**
   * Implicitly inject the [[HivemallStreamingOps]] into [[DStream]].
   */
  implicit def dataFrameToHivemallStreamingOps(ds: DStream[HivemallLabeledPoint])
      : HivemallStreamingOps = {
    new HivemallStreamingOps(ds)
  }
}
