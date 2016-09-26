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
package hivemall.ftvec.selection;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Preconditions;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Description(name = "snr", value = "_FUNC_(array<number> features, array<int> one-hot class label)"
        + " - Returns SNR values of each feature as array<double>")
public class SignalNoiseRatioUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        final ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isNumberListOI(OIs[0])) {
            throw new UDFArgumentTypeException(0,
                "Only array<number> type argument is acceptable but " + OIs[0].getTypeName()
                        + " was passed as `features`");
        }

        if (!HiveUtils.isListOI(OIs[1])
                || !HiveUtils.isIntegerOI(((ListObjectInspector) OIs[1]).getListElementObjectInspector())) {
            throw new UDFArgumentTypeException(1,
                "Only array<int> type argument is acceptable but " + OIs[1].getTypeName()
                        + " was passed as `labels`");
        }

        return new SignalNoiseRatioUDAFEvaluator();
    }

    static class SignalNoiseRatioUDAFEvaluator extends GenericUDAFEvaluator {
        // PARTIAL1 and COMPLETE
        private ListObjectInspector featuresOI;
        private PrimitiveObjectInspector featureOI;
        private ListObjectInspector labelsOI;
        private PrimitiveObjectInspector labelOI;

        // PARTIAL2 and FINAL
        private StructObjectInspector structOI;
        private StructField nsField, meanssField, variancessField;
        private ListObjectInspector nsOI;
        private LongObjectInspector nOI;
        private ListObjectInspector meanssOI;
        private ListObjectInspector meansOI;
        private DoubleObjectInspector meanOI;
        private ListObjectInspector variancessOI;
        private ListObjectInspector variancesOI;
        private DoubleObjectInspector varianceOI;

        @AggregationType(estimable = true)
        static class SignalNoiseRatioAggregationBuffer extends AbstractAggregationBuffer {
            long[] ns;
            double[][] meanss;
            double[][] variancess;

            @Override
            public int estimate() {
                return ns == null ? 0 : 8 * ns.length + 8 * meanss.length * meanss[0].length + 8
                        * variancess.length * variancess[0].length;
            }

            public void init(int nClasses, int nFeatures) {
                ns = new long[nClasses];
                meanss = new double[nClasses][nFeatures];
                variancess = new double[nClasses][nFeatures];
            }

            public void reset() {
                if (ns != null) {
                    Arrays.fill(ns, 0);
                    for (double[] means : meanss) {
                        Arrays.fill(means, 0.d);
                    }
                    for (double[] variances : variancess) {
                        Arrays.fill(variances, 0.d);
                    }
                }
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                featuresOI = HiveUtils.asListOI(OIs[0]);
                featureOI = HiveUtils.asDoubleCompatibleOI(featuresOI.getListElementObjectInspector());
                labelsOI = HiveUtils.asListOI(OIs[1]);
                labelOI = HiveUtils.asIntegerOI(labelsOI.getListElementObjectInspector());
            } else {
                structOI = (StructObjectInspector) OIs[0];
                nsField = structOI.getStructFieldRef("ns");
                nsOI = HiveUtils.asListOI(nsField.getFieldObjectInspector());
                nOI = HiveUtils.asLongOI(nsOI.getListElementObjectInspector());
                meanssField = structOI.getStructFieldRef("meanss");
                meanssOI = HiveUtils.asListOI(meanssField.getFieldObjectInspector());
                meansOI = HiveUtils.asListOI(meanssOI.getListElementObjectInspector());
                meanOI = HiveUtils.asDoubleOI(meansOI.getListElementObjectInspector());
                variancessField = structOI.getStructFieldRef("variancess");
                variancessOI = HiveUtils.asListOI(variancessField.getFieldObjectInspector());
                variancesOI = HiveUtils.asListOI(variancessOI.getListElementObjectInspector());
                varianceOI = HiveUtils.asDoubleOI(variancesOI.getListElementObjectInspector());
            }

            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector));
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)));
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)));
                return ObjectInspectorFactory.getStandardStructObjectInspector(
                    Arrays.asList("ns", "meanss", "variancess"), fieldOIs);
            } else {
                return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            }
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            final SignalNoiseRatioAggregationBuffer myAgg = new SignalNoiseRatioAggregationBuffer();
            reset(myAgg);
            return myAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            final SignalNoiseRatioAggregationBuffer myAgg = (SignalNoiseRatioAggregationBuffer) agg;
            myAgg.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            final Object featuresObj = parameters[0];
            final Object labelsObj = parameters[1];

            Preconditions.checkNotNull(featuresObj);
            Preconditions.checkNotNull(labelsObj);

            final SignalNoiseRatioAggregationBuffer myAgg = (SignalNoiseRatioAggregationBuffer) agg;

            // read class
            final List labels = labelsOI.getList(labelsObj);
            final int nClasses = labels.size();

            // to calc SNR between classes
            Preconditions.checkArgument(nClasses >= 2);

            int clazz = -1;
            for (int i = 0; i < nClasses; i++) {
                int label = PrimitiveObjectInspectorUtils.getInt(labels.get(i), labelOI);
                if (label == 1 && clazz == -1) {
                    clazz = i;
                } else if (label == 1) {
                    throw new UDFArgumentException(
                        "Specify one-hot vectorized array. Multiple hot elements found.");
                }
            }
            if (clazz == -1) {
                throw new UDFArgumentException(
                    "Specify one-hot vectorized array. Hot element not found.");
            }

            final List features = featuresOI.getList(featuresObj);
            final int nFeatures = features.size();

            Preconditions.checkArgument(nFeatures >= 1);

            if (myAgg.ns == null) {
                // init
                myAgg.init(nClasses, nFeatures);
            } else {
                Preconditions.checkArgument(nClasses == myAgg.ns.length);
                Preconditions.checkArgument(nFeatures == myAgg.meanss[0].length);
            }

            // calc incrementally
            final long n = myAgg.ns[clazz];
            myAgg.ns[clazz]++;
            for (int i = 0; i < nFeatures; i++) {
                final double x = PrimitiveObjectInspectorUtils.getDouble(features.get(i), featureOI);
                final double meanN = myAgg.meanss[clazz][i];
                final double varianceN = myAgg.variancess[clazz][i];
                myAgg.meanss[clazz][i] = (n * meanN + x) / (n + 1.d);
                myAgg.variancess[clazz][i] = (n * varianceN + (x - meanN)
                        * (x - myAgg.meanss[clazz][i]))
                        / (n + 1.d);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object other) throws HiveException {
            if (other == null) {
                return;
            }

            final SignalNoiseRatioAggregationBuffer myAgg = (SignalNoiseRatioAggregationBuffer) agg;

            final List ns = nsOI.getList(structOI.getStructFieldData(other, nsField));
            final List meanss = meanssOI.getList(structOI.getStructFieldData(other, meanssField));
            final List variancess = variancessOI.getList(structOI.getStructFieldData(other,
                variancessField));

            final int nClasses = ns.size();
            final int nFeatures = meansOI.getListLength(meanss.get(0));
            if (myAgg.ns == null) {
                // init
                myAgg.init(nClasses, nFeatures);
            }
            for (int i = 0; i < nClasses; i++) {
                final long n = myAgg.ns[i];
                final long m = PrimitiveObjectInspectorUtils.getLong(ns.get(i), nOI);
                final List means = meansOI.getList(meanss.get(i));
                final List variances = variancesOI.getList(variancess.get(i));

                myAgg.ns[i] += m;
                for (int j = 0; j < nFeatures; j++) {
                    final double meanN = myAgg.meanss[i][j];
                    final double meanM = PrimitiveObjectInspectorUtils.getDouble(means.get(j),
                        meanOI);
                    final double varianceN = myAgg.variancess[i][j];
                    final double varianceM = PrimitiveObjectInspectorUtils.getDouble(
                        variances.get(j), varianceOI);
                    myAgg.meanss[i][j] = (n * meanN + m * meanM) / (double) (n + m);
                    myAgg.variancess[i][j] = (varianceN * (n - 1) + varianceM * (m - 1) + FastMath.pow(
                        meanN - meanM, 2) * n * m / (n + m))
                            / (n + m - 1);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            final SignalNoiseRatioAggregationBuffer myAgg = (SignalNoiseRatioAggregationBuffer) agg;

            final Object[] partialResult = new Object[3];
            partialResult[0] = WritableUtils.toWritableList(myAgg.ns);
            final List<List<DoubleWritable>> meanss = new ArrayList<List<DoubleWritable>>();
            for (double[] means : myAgg.meanss) {
                meanss.add(WritableUtils.toWritableList(means));
            }
            partialResult[1] = meanss;
            final List<List<DoubleWritable>> variancess = new ArrayList<List<DoubleWritable>>();
            for (double[] variances : myAgg.variancess) {
                variancess.add(WritableUtils.toWritableList(variances));
            }
            partialResult[2] = variancess;
            return partialResult;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            final SignalNoiseRatioAggregationBuffer myAgg = (SignalNoiseRatioAggregationBuffer) agg;

            final int nClasses = myAgg.ns.length;
            final int nFeatures = myAgg.meanss[0].length;

            // calc SNR between classes each feature
            final double[] result = new double[nFeatures];
            final double[] sds = new double[nClasses]; // memo
            for (int i = 0; i < nFeatures; i++) {
                sds[0] = FastMath.sqrt(myAgg.variancess[0][i]);
                for (int j = 1; j < nClasses; j++) {
                    sds[j] = FastMath.sqrt(myAgg.variancess[j][i]);
                    if (Double.isNaN(sds[j])) {
                        continue;
                    }
                    for (int k = 0; k < j; k++) {
                        if (Double.isNaN(sds[k])) {
                            continue;
                        }
                        result[i] += FastMath.abs(myAgg.meanss[j][i] - myAgg.meanss[k][i])
                                / (sds[j] + sds[k]);
                    }
                }
            }

            // SUM(snr) GROUP BY feature
            return WritableUtils.toWritableList(result);
        }
    }
}
