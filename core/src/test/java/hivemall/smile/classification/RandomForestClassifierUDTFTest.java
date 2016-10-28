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
package hivemall.smile.classification;

import hivemall.utils.lang.mutable.MutableInt;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import smile.data.AttributeDataset;
import smile.data.parser.ArffParser;

public class RandomForestClassifierUDTFTest {

    @Test
    public void testIris() throws IOException, ParseException, HiveException {
        URL url = new URL(
            "https://gist.githubusercontent.com/myui/143fa9d05bd6e7db0114/raw/500f178316b802f1cade6e3bf8dc814a96e84b1e/iris.arff");
        InputStream is = new BufferedInputStream(url.openStream());

        ArffParser arffParser = new ArffParser();
        arffParser.setResponseIndex(4);

        AttributeDataset iris = arffParser.parse(is);
        int size = iris.size();
        double[][] x = iris.toArray(new double[size][]);
        int[] y = iris.toArray(new int[size]);

        RandomForestClassifierUDTF udtf = new RandomForestClassifierUDTF();
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-trees 49");
        udtf.initialize(new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
                PrimitiveObjectInspectorFactory.javaIntObjectInspector, param});

        final List<Double> xi = new ArrayList<Double>(x[0].length);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < x[i].length; j++) {
                xi.add(j, x[i][j]);
            }
            udtf.process(new Object[] {xi, y[i]});
            xi.clear();
        }

        final MutableInt count = new MutableInt(0);
        Collector collector = new Collector() {
            public void collect(Object input) throws HiveException {
                count.addValue(1);
            }
        };

        udtf.setCollector(collector);
        udtf.close();

        Assert.assertEquals(49, count.getValue());
    }

}
