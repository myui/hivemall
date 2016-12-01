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
package hivemall.ftvec.selection;

import hivemall.utils.hadoop.WritableUtils;

import java.util.ArrayList;
import java.util.List;

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

public class SignalNoiseRatioUDAFTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void snrBinaryClass() throws Exception {
        // this test is based on *subset* of iris data set
        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        final double[][] features = new double[][] { {5.1, 3.5, 1.4, 0.2}, {4.9, 3.d, 1.4, 0.2},
                {4.7, 3.2, 1.3, 0.2}, {7.d, 3.2, 4.7, 1.4}, {6.4, 3.2, 4.5, 1.5},
                {6.9, 3.1, 4.9, 1.5}};

        final int[][] labels = new int[][] { {1, 0}, {1, 0}, {1, 0}, {0, 1}, {0, 1}, {0, 1}};

        for (int i = 0; i < features.length; i++) {
            final List<IntWritable> labelList = new ArrayList<IntWritable>();
            for (int label : labels[i]) {
                labelList.add(new IntWritable(label));
            }
            evaluator.iterate(agg, new Object[] {WritableUtils.toWritableList(features[i]),
                    labelList});
        }

        @SuppressWarnings("unchecked")
        final List<DoubleWritable> resultObj = (List<DoubleWritable>) evaluator.terminate(agg);
        final int size = resultObj.size();
        final double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = resultObj.get(i).get();
        }

        // compare with result by numpy
        final double[] answer = new double[] {4.38425236, 0.26390002, 15.83984511, 26.87005769};

        Assert.assertArrayEquals(answer, result, 1e-5);
    }

    @Test
    public void snrMultipleClassNormalCase() throws Exception {
        // this test is based on *subset* of iris data set
        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        final double[][] features = new double[][] { {5.1, 3.5, 1.4, 0.2}, {4.9, 3.d, 1.4, 0.2},
                {7.d, 3.2, 4.7, 1.4}, {6.4, 3.2, 4.5, 1.5}, {6.3, 3.3, 6.d, 2.5},
                {5.8, 2.7, 5.1, 1.9}};

        final int[][] labels = new int[][] { {1, 0, 0}, {1, 0, 0}, {0, 1, 0}, {0, 1, 0}, {0, 0, 1},
                {0, 0, 1}};

        for (int i = 0; i < features.length; i++) {
            final List<IntWritable> labelList = new ArrayList<IntWritable>();
            for (int label : labels[i]) {
                labelList.add(new IntWritable(label));
            }
            evaluator.iterate(agg, new Object[] {WritableUtils.toWritableList(features[i]),
                    labelList});
        }

        @SuppressWarnings("unchecked")
        final List<DoubleWritable> resultObj = (List<DoubleWritable>) evaluator.terminate(agg);
        final int size = resultObj.size();
        final double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = resultObj.get(i).get();
        }

        // compare with result by scikit-learn
        final double[] answer = new double[] {8.43181818, 1.32121212, 42.94949495, 33.80952381};

        Assert.assertArrayEquals(answer, result, 1e-5);
    }

    @Test
    public void snrMultipleClassCornerCase0() throws Exception {
        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        // all c0[0] and c1[0] are equal
        // all c1[1] and c2[1] are equal
        // all c*[2] are equal
        // all c*[3] are different
        final double[][] features = new double[][] { {3.5, 1.4, 0.3, 5.1}, {3.5, 1.5, 0.3, 5.2},
                {3.5, 4.5, 0.3, 7.d}, {3.5, 4.5, 0.3, 6.4}, {3.3, 4.5, 0.3, 6.3}};

        final int[][] labels = new int[][] { {1, 0, 0}, {1, 0, 0}, // class `0`
                {0, 1, 0}, {0, 1, 0}, // class `1`
                {0, 0, 1}}; // class `2`, only single entry

        for (int i = 0; i < features.length; i++) {
            final List<IntWritable> labelList = new ArrayList<IntWritable>();
            for (int label : labels[i]) {
                labelList.add(new IntWritable(label));
            }
            evaluator.iterate(agg, new Object[] {WritableUtils.toWritableList(features[i]),
                    labelList});
        }

        @SuppressWarnings("unchecked")
        final List<DoubleWritable> resultObj = (List<DoubleWritable>) evaluator.terminate(agg);
        final int size = resultObj.size();
        final double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = resultObj.get(i).get();
        }

        final double[] answer = new double[] {Double.POSITIVE_INFINITY, 121.99999999999989, 0.d,
                28.761904761904734};

        Assert.assertArrayEquals(answer, result, 1e-5);
    }

    @Test
    public void snrMultipleClassCornerCase1() throws Exception {
        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        final double[][] features = new double[][] { {5.1, 3.5, 1.4, 0.2}, {4.9, 3.d, 1.4, 0.2},
                {7.d, 3.2, 4.7, 1.4}, {6.3, 3.3, 6.d, 2.5}, {6.4, 3.2, 4.5, 1.5}};

        // has multiple single entries
        final int[][] labels = new int[][] { {1, 0, 0}, {1, 0, 0}, {1, 0, 0}, // class `0`
                {0, 1, 0}, // class `1`, only single entry
                {0, 0, 1}}; // class `2`, only single entry

        for (int i = 0; i < features.length; i++) {
            final List<IntWritable> labelList = new ArrayList<IntWritable>();
            for (int label : labels[i]) {
                labelList.add(new IntWritable(label));
            }
            evaluator.iterate(agg, new Object[] {WritableUtils.toWritableList(features[i]),
                    labelList});
        }

        @SuppressWarnings("unchecked")
        final List<DoubleWritable> resultObj = (List<DoubleWritable>) evaluator.terminate(agg);
        final List<Double> result = new ArrayList<Double>();
        for (DoubleWritable dw : resultObj) {
            result.add(dw.get());
        }

        Assert.assertFalse(result.contains(Double.POSITIVE_INFINITY));
    }

    @Test
    public void snrMultipleClassCornerCase2() throws Exception {
        final SignalNoiseRatioUDAF snr = new SignalNoiseRatioUDAF();
        final ObjectInspector[] OIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector),
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableIntObjectInspector)};
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator evaluator = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator) snr.getEvaluator(new SimpleGenericUDAFParameterInfo(
            OIs, false, false));
        evaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, OIs);
        final SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer agg = (SignalNoiseRatioUDAF.SignalNoiseRatioUDAFEvaluator.SignalNoiseRatioAggregationBuffer) evaluator.getNewAggregationBuffer();
        evaluator.reset(agg);

        // all [0] are equal
        // all [1] are equal *each class*
        final double[][] features = new double[][] { {1.d, 1.d, 1.4, 0.2}, {1.d, 1.d, 1.4, 0.2},
                {1.d, 2.d, 4.7, 1.4}, {1.d, 2.d, 4.5, 1.5}, {1.d, 3.d, 6.d, 2.5},
                {1.d, 3.d, 5.1, 1.9}};

        final int[][] labels = new int[][] { {1, 0, 0}, {1, 0, 0}, {0, 1, 0}, {0, 1, 0}, {0, 0, 1},
                {0, 0, 1}};

        for (int i = 0; i < features.length; i++) {
            final List<IntWritable> labelList = new ArrayList<IntWritable>();
            for (int label : labels[i]) {
                labelList.add(new IntWritable(label));
            }
            evaluator.iterate(agg, new Object[] {WritableUtils.toWritableList(features[i]),
                    labelList});
        }

        @SuppressWarnings("unchecked")
        final List<DoubleWritable> resultObj = (List<DoubleWritable>) evaluator.terminate(agg);
        final int size = resultObj.size();
        final double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = resultObj.get(i).get();
        }

        final double[] answer = new double[] {0.d, Double.POSITIVE_INFINITY, 42.94949495,
                33.80952381};

        Assert.assertArrayEquals(answer, result, 1e-5);
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
