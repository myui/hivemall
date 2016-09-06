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

package org.apache.spark.sql.hive.source

import java.io.File
import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.mapreduce._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

private[source] final class XGBoostOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter {

  private val hadoopConf = new SerializableConfiguration(new Configuration())

  override def write(row: Row): Unit = {
    val modelId= row.getString(0)
    val model = row.get(1).asInstanceOf[Array[Byte]]
    val filePath = new Path(new URI(s"$path/$modelId"))
    val fs = filePath.getFileSystem(hadoopConf.value)
    val outputFile = fs.create(filePath)
    outputFile.write(model)
    outputFile.close()
  }

  override def close(): Unit = {}
}

final class XGBoostFileFormat extends FileFormat with DataSourceRegister {

  override def shortName(): String = "libxgboost"

  override def toString: String = "XGBoost"

  private def verifySchema(dataSchema: StructType): Unit = {
    if (
      dataSchema.size != 2 ||
        !dataSchema(0).dataType.sameType(StringType) ||
        !dataSchema(1).dataType.sameType(BinaryType)
    ) {
      throw new IOException(s"Illegal schema for XGBoost data, schema=$dataSchema")
    }
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Some(
      StructType(
        StructField("model_id", StringType, nullable = false) ::
        StructField("pred_model", BinaryType, nullable = false) :: Nil)
    )
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          bucketId: Option[Int],
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        if (bucketId.isDefined) {
          sys.error("XGBoostFileFormat doesn't support bucketing")
        }
        new XGBoostOutputWriter(path, dataSchema, context)
      }
    }
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    verifySchema(dataSchema)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val model = new Array[Byte](file.length.asInstanceOf[Int])
      val filePath = new Path(new URI(file.filePath))
      val fs = filePath.getFileSystem(broadcastedHadoopConf.value.value)

      var in: FSDataInputStream = null
      try {
        in = fs.open(filePath)
        IOUtils.readFully(in, model, 0, model.length)
      } finally {
        IOUtils.closeStream(in)
      }

      val converter = RowEncoder(dataSchema)
      val fullOutput = dataSchema.map { f =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()
      }
      val requiredOutput = fullOutput.filter { a =>
        requiredSchema.fieldNames.contains(a.name)
      }
      val requiredColumns = GenerateUnsafeProjection.generate(requiredOutput, fullOutput)
      (requiredColumns(
        converter.toRow(Row(new File(file.filePath).getName, model)))
          :: Nil
        ).toIterator
    }
  }
}
