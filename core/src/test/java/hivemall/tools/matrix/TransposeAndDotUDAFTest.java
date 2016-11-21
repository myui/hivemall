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
package hivemall.tools.matrix;

import hivemall.utils.hadoop.WritableUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class TransposeAndDotUDAFTest {

    @Test
    public void test() throws Exception {
        final TransposeAndDotUDAF tad = new TransposeAndDotUDAF();

        final double[][] matrix0 = new double[][] { {1, -2}, {-1, 3}};
        final double[][] matrix1 = new double[][] { {1, 2}, {3, 4}};

        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)};
        final GenericUDAFEvaluator evaluator = tad.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        TransposeAndDotUDAF.TransposeAndDotUDAFEvaluator.TransposeAndDotAggregationBuffer agg = (TransposeAndDotUDAF.TransposeAndDotUDAFEvaluator.TransposeAndDotAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);
        for (int i = 0; i < matrix0.length; i++) {
            evaluator.iterate(agg, new Object[] {WritableUtils.toWritableList(matrix0[i]),
                    WritableUtils.toWritableList(matrix1[i])});
        }

        final double[][] answer = new double[][] { {-2.0, -2.0}, {7.0, 8.0}};

        for (int i = 0; i < answer.length; i++) {
            Assert.assertArrayEquals(answer[i], agg.aggMatrix[i], 0.d);
        }
    }
}
