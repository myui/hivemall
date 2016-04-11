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

package hivemall.mix.server

import java.util.Random
import java.util.concurrent.{TimeUnit, ExecutorService, Executors}
import java.util.logging.Logger

import org.scalatest.{BeforeAndAfter, FunSuite}

import hivemall.io.{DenseModel, PredictionModel, WeightValue}
import hivemall.mix.MixMessage.MixEventName
import hivemall.mix.client.MixClient
import hivemall.mix.server.MixServer.ServerState
import hivemall.utils.io.IOUtils
import hivemall.utils.lang.CommandLineUtils
import hivemall.utils.net.NetUtils

class MixServerSuite extends FunSuite with BeforeAndAfter {

  private[this] var server: MixServer = _
  private[this] var executor : ExecutorService = _
  private[this] var port: Int = _

  private[this] val rand = new Random(43)
  private[this] val counter = Stream.from(0).iterator

  private[this] val eachTestTime = 100
  private[this] val logger =
    Logger.getLogger(classOf[MixServerSuite].getName)

  before {
    this.port = NetUtils.getAvailablePort
    this.server = new MixServer(
      CommandLineUtils.parseOptions(
        Array("-port", s"${port}", "-sync_threshold", "3"),
        MixServer.getOptions()
      )
    )
    this.executor = Executors.newSingleThreadExecutor
    this.executor.submit(server)
    var retry = 0
    while (server.getState() != ServerState.RUNNING && retry < 50) {
      Thread.sleep(1000L)
      retry += 1
    }
    assert(server.getState == ServerState.RUNNING)
  }

  after { this.executor.shutdown() }

  private[this] def clientDriver(groupId: String, model: PredictionModel, numMsg: Int = 1000000): Unit = {
    var client: MixClient = null
    try {
      client = new MixClient(MixEventName.average, groupId, s"localhost:${port}", false, 2, model)
      model.configureMix(client, false)
      model.configureClock()

      for (_ <- 0 until numMsg) {
        val feature = Integer.valueOf(rand.nextInt(model.size))
        model.set(feature, new WeightValue(1.0f))
      }

      while (true) { Thread.sleep(eachTestTime * 1000 + 100L) }
      assert(model.getNumMixed > 0)
    } finally {
      IOUtils.closeQuietly(client)
    }
  }

  private[this] def fixedGroup: (String, () => String) =
    ("fixed", () => "fixed")
  private[this] def uniqueGroup: (String, () => String) =
    ("unique", () => s"${counter.next}")

  Seq(65536).map { ndims =>
    Seq(4).map { nclient =>
      Seq(fixedGroup, uniqueGroup).map { id =>
        val testName = s"dense-dim:${ndims}-clinet:${nclient}-${id._1}"
        ignore(testName) {
          val clients = Executors.newCachedThreadPool()
          val numClients = nclient
          val models = (0 until numClients).map(i => new DenseModel(ndims, false))
          (0 until numClients).map { i =>
            clients.submit(new Runnable() {
              override def run(): Unit = {
                try {
                  clientDriver(
                    s"${testName}-${id._2}",
                    models(i)
                  )
                } catch {
                  case e: InterruptedException =>
                    assert(false, e.getMessage)
                }
              }
            })
          }
          clients.awaitTermination(eachTestTime, TimeUnit.SECONDS)
          clients.shutdown()
          val nMixes = models.map(d => d.getNumMixed).reduce(_ + _)
          logger.info(s"${testName} --> ${(nMixes + 0.0) / eachTestTime} mixes/s")
        }
      }
    }
  }
}
