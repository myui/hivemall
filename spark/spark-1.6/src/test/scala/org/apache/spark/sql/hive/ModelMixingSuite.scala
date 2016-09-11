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

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.commons.cli.Options
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.HivemallLabeledPoint
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.hive.HivemallOps._
import org.apache.spark.sql.hive.HivemallUtils._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.test.TestUtils.{benchmark, invokeFunc}
import org.scalatest.BeforeAndAfter

import hivemall.mix.server.MixServer
import hivemall.mix.server.MixServer.ServerState
import hivemall.utils.lang.CommandLineUtils
import hivemall.utils.net.NetUtils

final class ModelMixingSuite extends SparkFunSuite with BeforeAndAfter {

  // Load A9a training and test data
  val a9aLineParser = (line: String) => {
    val elements = line.split(" ")
    val (label, features) = (elements.head, elements.tail)
    HivemallLabeledPoint(if (label == "+1") 1.0f else 0.0f, features)
  }

  lazy val trainA9aData: DataFrame =
    getDataFromURI(
      new URL("http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a9a").openStream,
      a9aLineParser)

  lazy val testA9aData: DataFrame =
    getDataFromURI(
      new URL("http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/a9a.t").openStream,
      a9aLineParser)

  // Load A9a training and test data
  val kdd2010aLineParser = (line: String) => {
    val elements = line.split(" ")
    val (label, features) = (elements.head, elements.tail)
    HivemallLabeledPoint(if (label == "1") 1.0f else 0.0f, features)
  }

  lazy val trainKdd2010aData: DataFrame =
    getDataFromURI(
      new CompressorStreamFactory().createCompressorInputStream(
        new BufferedInputStream(
          new URL("http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/kdda.bz2")
            .openStream
        )
      ),
      kdd2010aLineParser,
      8)

  lazy val testKdd2010aData: DataFrame =
    getDataFromURI(
      new CompressorStreamFactory().createCompressorInputStream(
        new BufferedInputStream(
          new URL("http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary/kdda.t.bz2")
            .openStream
        )
      ),
      kdd2010aLineParser,
      8)

  // Placeholder for a mix server
  var mixServExec: ExecutorService = _
  var assignedPort: Int = _

  private def getDataFromURI(
      in: InputStream, lineParseFunc: String => HivemallLabeledPoint, numPart: Int = 2): DataFrame = {
    val reader = new BufferedReader(new InputStreamReader(in))
    try {
      // Cache all data because stream closed soon
      val lines = FileIterator(reader.readLine()).toSeq
      val rdd = TestHive.sparkContext.parallelize(lines, numPart).map(lineParseFunc)
      val df = rdd.toDF.cache
      df.foreach(_ => {})
      df
    } finally {
      reader.close()
    }
  }

  before {
    assert(mixServExec == null)

    // Launch a MIX server as thread
    assignedPort = NetUtils.getAvailablePort
    val method = classOf[MixServer].getDeclaredMethod("getOptions")
    method.setAccessible(true)
    val options = method.invoke(null).asInstanceOf[Options]
    val cl = CommandLineUtils.parseOptions(
      Array(
        "-port", Integer.toString(assignedPort),
        "-sync_threshold", "1"
      ),
      options
    )
    val server = new MixServer(cl)
    mixServExec = Executors.newSingleThreadExecutor()
    mixServExec.submit(server)
    var retry = 0
    while (server.getState() != ServerState.RUNNING && retry < 32) {
      Thread.sleep(100L)
      retry += 1
    }
    assert(ServerState.RUNNING == server.getState)
  }

  after {
    mixServExec.shutdownNow()
    mixServExec = null
  }

  benchmark("model mixing test w/ regression") {
    Seq(
      "train_adadelta",
      "train_adagrad",
      "train_arow_regr",
      "train_arowe_regr",
      "train_arowe2_regr",
      "train_logregr",
      "train_pa1_regr",
      "train_pa1a_regr",
      "train_pa2_regr",
      "train_pa2a_regr"
    ).map { func =>
      // Build a model
      val model = {
        val groupId = s"${TestHive.sparkContext.applicationId}-${UUID.randomUUID}"
        val res = invokeFunc(new HivemallOps(trainA9aData.part_amplify(1)), func,
          Seq[Column](add_bias($"features"), $"label",
            s"-mix localhost:${assignedPort} -mix_session ${groupId} -mix_threshold 2 -mix_cancel"))
        if (!res.columns.contains("conv")) {
          res.groupby("feature").agg("weight"->"avg")
        } else {
          res.groupby("feature").argmin_kld("weight", "conv")
        }
      }.as("feature", "weight")

      // Data preparation
      val testDf = testA9aData
        .select(rowid(), $"label".as("target"), $"features")
        .cache

      val testDf_exploded = testDf
        .explode_array($"features")
        .select($"rowid", extract_feature($"feature"), extract_weight($"feature"))

      // Do prediction
      val predict = testDf_exploded
        .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
        .select($"rowid", ($"weight" * $"value").as("value"))
        .groupby("rowid").sum("value")
        .as("rowid", "predicted")

      // Evaluation
      val eval = predict
        .join(testDf, predict("rowid") === testDf("rowid"))
        .groupby()
        .agg(Map("target"->"avg", "predicted"->"avg"))
        .as("target", "predicted")

      val (target, predicted) = eval.map {
        case Row(target: Double, predicted: Double) => (target, predicted)
      }.first

      println(s"func:${func} target:${target} predicted:${predicted} "
        + s"diff:${Math.abs(target - predicted)}")

      testDf.unpersist()
    }
  }

  benchmark("model mixing test w/ binary classification") {
    Seq(
      "train_perceptron",
      "train_pa",
      "train_pa1",
      "train_pa2",
      "train_cw",
      "train_arow",
      "train_arowh",
      "train_scw",
      "train_scw2",
      "train_adagrad_rda"
    ).map { func =>
      // Build a model
      val model = {
        val groupId = s"${TestHive.sparkContext.applicationId}-${UUID.randomUUID}"
        val res = invokeFunc(new HivemallOps(trainKdd2010aData.part_amplify(1)), func,
          Seq[Column](add_bias($"features"), $"label",
            s"-mix localhost:${assignedPort} -mix_session ${groupId} -mix_threshold 2 -mix_cancel"))
        if (!res.columns.contains("conv")) {
          res.groupby("feature").agg("weight"->"avg")
        } else {
          res.groupby("feature").argmin_kld("weight", "conv")
        }
      }.as("feature", "weight")

      // Data preparation
      val testDf = testKdd2010aData
        .select(rowid(), $"label".as("target"), $"features")
        .cache

      val testDf_exploded = testDf
        .explode_array($"features")
        .select($"rowid", extract_feature($"feature"), extract_weight($"feature"))

      // Do prediction
      val predict = testDf_exploded
        .join(model, testDf_exploded("feature") === model("feature"), "LEFT_OUTER")
        .select($"rowid", ($"weight" * $"value").as("value"))
        .groupby("rowid").sum("value")
        .select($"rowid", when(sigmoid($"sum(value)") > 0.50, 1.0).otherwise(0.0))
        .as("rowid", "predicted")

      // Evaluation
      val eval = predict
        .join(testDf, predict("rowid") === testDf("rowid"))
        .where($"target" === $"predicted")

      println(s"func:${func} precision:${(eval.count + 0.0) / predict.count}")

      testDf.unpersist()
      predict.unpersist()
    }
  }
}

object FileIterator {

  def apply[A](f: => A) = new Iterator[A] {
    var opt = Option(f)
    def hasNext = opt.nonEmpty
    def next() = {
      val r = opt.get
      opt = Option(f)
      r
    }
  }
}
