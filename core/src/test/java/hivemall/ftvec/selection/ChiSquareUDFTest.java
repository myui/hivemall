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
package hivemall.ftvec.selection;

import hivemall.utils.hadoop.WritableUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ChiSquareUDFTest {

    @Test
    public void test() throws Exception {
        // this test is based on iris data set
        final ChiSquareUDF chi2 = new ChiSquareUDF();
        final List<List<DoubleWritable>> observed = new ArrayList<List<DoubleWritable>>();
        final List<List<DoubleWritable>> expected = new ArrayList<List<DoubleWritable>>();
        final GenericUDF.DeferredObject[] dObjs = new GenericUDF.DeferredObject[] {
                new GenericUDF.DeferredJavaObject(observed),
                new GenericUDF.DeferredJavaObject(expected)};

        final double[][] matrix0 = new double[][] {
                {250.29999999999998, 170.90000000000003, 73.2, 12.199999999999996},
                {296.8, 138.50000000000003, 212.99999999999997, 66.3},
                {329.3999999999999, 148.7, 277.59999999999997, 101.29999999999998}};
        final double[][] matrix1 = new double[][] {
                {292.1666753739119, 152.70000455081467, 187.93333893418327, 59.93333511948589},
                {292.1666753739119, 152.70000455081467, 187.93333893418327, 59.93333511948589},
                {292.1666753739119, 152.70000455081467, 187.93333893418327, 59.93333511948589}};

        for (double[] row : matrix0) {
            observed.add(WritableUtils.toWritableList(row));
        }
        for (double[] row : matrix1) {
            expected.add(WritableUtils.toWritableList(row));
        }

        chi2.initialize(new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)),
                ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector))});
        final Object[] result = (Object[]) chi2.evaluate(dObjs);
        final double[] result0 = new double[matrix0[0].length];
        final double[] result1 = new double[matrix0[0].length];
        for (int i = 0; i < result0.length; i++) {
            result0[i] = Double.valueOf(((List) result[0]).get(i).toString());
            result1[i] = Double.valueOf(((List) result[1]).get(i).toString());
        }

        final double[] answer0 = new double[] {10.817820878493995, 3.5944990176817315,
                116.16984746363957, 67.24482558215503};
        final double[] answer1 = new double[] {0.004476514990225833, 0.16575416718561453, 0.d,
                2.55351295663786e-15};

        Assert.assertArrayEquals(answer0, result0, 0.d);
        Assert.assertArrayEquals(answer1, result1, 0.d);
    }
}
