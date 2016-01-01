/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.smile.classification;

import hivemall.utils.lang.mutable.MutableInt;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
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

        for (int i = 0; i < size; i++) {
            List<Double> xi = new DoubleArrayList(x[i]);
            udtf.process(new Object[] {xi, y[i]});
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
