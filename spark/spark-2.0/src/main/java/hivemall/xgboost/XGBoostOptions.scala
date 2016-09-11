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

package hivemall.xgboost

import scala.collection.mutable

import org.apache.commons.cli.Options
import org.apache.spark.annotation.AlphaComponent

/**
 * :: AlphaComponent ::
 * An utility class to generate a sequence of options used in XGBoost.
 */
@AlphaComponent
case class XGBoostOptions() {
  private val params: mutable.Map[String, String] = mutable.Map.empty
  private val options: Options = {
    new XGBoostUDTF() {
      def options(): Options = super.getOptions()
    }.options()
  }

  private def isValidKey(key: String): Boolean = {
    // TODO: Is there another way to handle all the XGBoost options?
    options.hasOption(key) || key == "num_class"
  }

  def set(key: String, value: String): XGBoostOptions = {
    require(isValidKey(key), s"non-existing key detected in XGBoost options: ${key}")
    params.put(key, value)
    this
  }

  def help(): Unit = {
    import scala.collection.JavaConversions._
    options.getOptions.map { case option => println(option) }
  }

  override def toString(): String = {
    params.map { case (key, value) => s"-$key $value" }.mkString(" ")
  }
}
