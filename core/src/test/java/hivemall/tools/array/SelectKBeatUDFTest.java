/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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
package hivemall.tools.array;

import hivemall.utils.hadoop.WritableUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SelectKBeatUDFTest {

    @Test
    public void test() throws Exception {
        final SelectKBestUDF selectKBest = new SelectKBestUDF();
        final int k = 2;
        final double[] data = new double[] {250.29999999999998, 170.90000000000003, 73.2,
                12.199999999999996};
        final double[] importanceList = new double[] {292.1666753739119, 152.70000455081467,
                187.93333893418327, 59.93333511948589};

        final GenericUDF.DeferredObject[] dObjs = new GenericUDF.DeferredObject[] {
                new GenericUDF.DeferredJavaObject(WritableUtils.toWritableList(data)),
                new GenericUDF.DeferredJavaObject(WritableUtils.toWritableList(importanceList)),
                new GenericUDF.DeferredJavaObject(new IntWritable(k))};

        selectKBest.initialize(new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                PrimitiveObjectInspectorFactory.writableIntObjectInspector});
        final List resultObj = (List) selectKBest.evaluate(dObjs);

        Assert.assertEquals(resultObj.size(), k);

        final double[] result = new double[k];
        for (int i = 0; i < k; i++) {
            result[i] = Double.valueOf(resultObj.get(i).toString());
        }

        final double[] answer = new double[] {250.29999999999998, 73.2};

        Assert.assertArrayEquals(answer, result, 0.d);
    }
}
