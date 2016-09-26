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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

public class SignalNoiseRatioUDAFTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void test() throws Exception {
        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        final double[][] featuress = new double[][] { {5.1, 3.5, 1.4, 0.2}, {4.9, 3.d, 1.4, 0.2},
                {7.d, 3.2, 4.7, 1.4}, {6.4, 3.2, 4.5, 1.5}, {6.3, 3.3, 6.d, 2.5},
                {5.8, 2.7, 5.1, 1.9}};

        final int[][] labelss = new int[][] { {1, 0, 0}, {1, 0, 0}, {0, 1, 0}, {0, 1, 0},
                {0, 0, 1}, {0, 0, 1}};

        for (int i = 0; i < featuress.length; i++) {
            final List<IntWritable> labels = new ArrayList<IntWritable>();
            for (int label : labelss[i]) {
                labels.add(new IntWritable(label));
            }
            evaluator.iterate(agg,
                new Object[] {WritableUtils.toWritableList(featuress[i]), labels});
        }

        @SuppressWarnings("unchecked")
        final List<DoubleWritable> resultObj = (ArrayList<DoubleWritable>) evaluator.terminate(agg);
        final int size = resultObj.size();
        final double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = resultObj.get(i).get();
        }
        final double[] answer = new double[] {8.431818181818192, 1.3212121212121217,
                42.94949494949499, 33.80952380952378};
        Assert.assertArrayEquals(answer, result, 0.d);
    }

    @Test
    public void shouldFail0() throws Exception {
        expectedException.expect(UDFArgumentException.class);

        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        final double[][] featuress = new double[][] { {5.1, 3.5, 1.4, 0.2}, {4.9, 3.d, 1.4, 0.2},
                {7.d, 3.2, 4.7, 1.4}, {6.4, 3.2, 4.5, 1.5}, {6.3, 3.3, 6.d, 2.5},
                {5.8, 2.7, 5.1, 1.9}};

        final int[][] labelss = new int[][] { {0, 0, 0}, // cause UDFArgumentException
                {1, 0, 0}, {0, 1, 0}, {0, 1, 0}, {0, 0, 1}, {0, 0, 1}};

        for (int i = 0; i < featuress.length; i++) {
            final List<IntWritable> labels = new ArrayList<IntWritable>();
            for (int label : labelss[i]) {
                labels.add(new IntWritable(label));
            }
            evaluator.iterate(agg,
                new Object[] {WritableUtils.toWritableList(featuress[i]), labels});
        }
    }

    @Test
    public void shouldFail1() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        final double[][] featuress = new double[][] { {5.1, 3.5, 1.4, 0.2},
                {4.9, 3.d, 1.4}, // cause IllegalArgumentException
                {7.d, 3.2, 4.7, 1.4}, {6.4, 3.2, 4.5, 1.5}, {6.3, 3.3, 6.d, 2.5},
                {5.8, 2.7, 5.1, 1.9}};

        final int[][] labelss = new int[][] { {1, 0, 0}, {1, 0, 0}, {0, 1, 0}, {0, 1, 0},
                {0, 0, 1}, {0, 0, 1}};

        for (int i = 0; i < featuress.length; i++) {
            final List<IntWritable> labels = new ArrayList<IntWritable>();
            for (int label : labelss[i]) {
                labels.add(new IntWritable(label));
            }
            evaluator.iterate(agg,
                new Object[] {WritableUtils.toWritableList(featuress[i]), labels});
        }
    }

    @Test
    public void shouldFail2() throws Exception {
        expectedException.expect(IllegalArgumentException.class);

        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        final double[][] featuress = new double[][] { {5.1, 3.5, 1.4, 0.2}, {4.9, 3.d, 1.4, 0.2},
                {7.d, 3.2, 4.7, 1.4}, {6.4, 3.2, 4.5, 1.5}, {6.3, 3.3, 6.d, 2.5},
                {5.8, 2.7, 5.1, 1.9}};

        final int[][] labelss = new int[][] { {1}, {1}, {1}, {1}, {1}, {1}}; // cause IllegalArgumentException

        for (int i = 0; i < featuress.length; i++) {
            final List<IntWritable> labels = new ArrayList<IntWritable>();
            for (int label : labelss[i]) {
                labels.add(new IntWritable(label));
            }
            evaluator.iterate(agg,
                new Object[] {WritableUtils.toWritableList(featuress[i]), labels});
        }
    }
}
