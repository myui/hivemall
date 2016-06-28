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

package org.apache.spark.sql.hive

import org.apache.spark.sql.Row
import org.apache.spark.test.HivemallQueryTest

final class HiveUdfSuite extends HivemallQueryTest {

  import hiveContext.implicits._
  import hiveContext._

  test("hivemall_version") {
    sql(s"""
         | CREATE TEMPORARY FUNCTION hivemall_version
         |   AS '${classOf[hivemall.HivemallVersionUDF].getName}'
       """.stripMargin)

    checkAnswer(
      sql(s"SELECT DISTINCT hivemall_version()"),
      Row("0.4.2-rc.2")
    )

    // sql("DROP TEMPORARY FUNCTION IF EXISTS hivemall_version")
    // reset()
  }

  test("train_logregr") {
    TinyTrainData.registerTempTable("TinyTrainData")
    sql(s"""
         | CREATE TEMPORARY FUNCTION train_logregr
         |   AS '${classOf[hivemall.regression.LogressUDTF].getName}'
       """.stripMargin)
    sql(s"""
         | CREATE TEMPORARY FUNCTION add_bias
         |   AS '${classOf[hivemall.ftvec.AddBiasUDFWrapper].getName}'
       """.stripMargin)

    val model = sql(
      s"""
         | SELECT feature, AVG(weight) AS weight
         |   FROM (
         |       SELECT train_logregr(add_bias(features), label) AS (feature, weight)
         |         FROM TinyTrainData
         |     ) t
         |   GROUP BY feature
       """.stripMargin)

    checkAnswer(
      model.select($"feature"),
      Seq(Row("0"), Row("1"), Row("2"))
    )

    // TODO: Why 'train_logregr' is not registered in HiveMetaStore?
    // ERROR RetryingHMSHandler: MetaException(message:NoSuchObjectException
    //   (message:Function default.train_logregr does not exist))
    //
    // hiveContext.sql("DROP TEMPORARY FUNCTION IF EXISTS train_logregr")
    // hiveContext.reset()
  }
}
