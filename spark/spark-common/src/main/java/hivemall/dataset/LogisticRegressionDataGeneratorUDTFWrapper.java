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
package hivemall.dataset;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import hivemall.UDTFWithOptions;

/**
 * A wrapper of [[hivemall.dataset.LogisticRegressionDataGeneratorUDTF]]. This wrapper is needed
 * because Spark cannot handle HadoopUtils#getTaskId() correctly.
 */
@Description(name = "lr_datagen",
        value = "_FUNC_(options string) - Generates a logistic regression dataset")
public final class LogisticRegressionDataGeneratorUDTFWrapper extends UDTFWithOptions {
    private transient LogisticRegressionDataGeneratorUDTF udtf = new LogisticRegressionDataGeneratorUDTF();

    @Override
    protected Options getOptions() {
        Options options = null;
        try {
            Method m = udtf.getClass().getDeclaredMethod("getOptions");
            m.setAccessible(true);
            options = (Options) m.invoke(udtf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return options;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] objectInspectors)
            throws UDFArgumentException {
        CommandLine commands = null;
        try {
            Method m = udtf.getClass().getDeclaredMethod("processOptions");
            m.setAccessible(true);
            commands = (CommandLine) m.invoke(udtf, objectInspectors);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return commands;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        try {
            // Extract a collector for LogisticRegressionDataGeneratorUDTF
            Field collector = GenericUDTF.class.getDeclaredField("collector");
            collector.setAccessible(true);
            udtf.setCollector((Collector) collector.get(this));

            // To avoid HadoopUtils#getTaskId()
            Class<?> clazz = udtf.getClass();
            Field rnd1 = clazz.getDeclaredField("rnd1");
            Field rnd2 = clazz.getDeclaredField("rnd2");
            Field r_seed = clazz.getDeclaredField("r_seed");
            r_seed.setAccessible(true);
            final long seed = r_seed.getLong(udtf) + (int) Thread.currentThread().getId();
            rnd1.setAccessible(true);
            rnd2.setAccessible(true);
            rnd1.set(udtf, new Random(seed));
            rnd2.set(udtf, new Random(seed + 1));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return udtf.initialize(argOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        udtf.process(objects);
    }

    @Override
    public void close() throws HiveException {
        udtf.close();
    }
}
