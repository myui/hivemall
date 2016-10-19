/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.ftvec.binning;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Preconditions;
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
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Description(name = "build_bins",
        value = "_FUNC_(number weight, const int num_of_bins[, const boolean auto_shrink = false])"
                + " - Return quantiles representing bins: array<double>")
public final class BuildBinsUDAF extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 2 && OIs.length != 3) {
            throw new UDFArgumentLengthException("Specify two or three arguments.");
        }

        if (!HiveUtils.isNumberOI(OIs[0])) {
            throw new UDFArgumentTypeException(0, "Only number type argument is acceptable but "
                    + OIs[0].getTypeName() + " was passed as `weight`");
        }

        if (!HiveUtils.isIntegerOI(OIs[1])) {
            throw new UDFArgumentTypeException(1, "Only int type argument is acceptable but "
                    + OIs[1].getTypeName() + " was passed as `num_of_bins`");
        }

        if (OIs.length == 3) {
            if (!HiveUtils.isBooleanOI(OIs[2])) {
                throw new UDFArgumentTypeException(2,
                    "Only boolean type argument is acceptable but " + OIs[2].getTypeName()
                            + " was passed as `auto_shrink`");
            }
        }

        return new BuildBinsUDAFEvaluator();
    }

    private static class BuildBinsUDAFEvaluator extends GenericUDAFEvaluator {
        // PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector weightOI;

        // PARTIAL2 and FINAL
        private StructObjectInspector structOI;
        private StructField autoShrinkField, histogramField, quantilesField;
        private BooleanObjectInspector autoShrinkOI;
        private StandardListObjectInspector histogramOI;
        private DoubleObjectInspector histogramElOI;
        private StandardListObjectInspector quantilesOI;
        private DoubleObjectInspector quantileOI;

        private int nBGBins = 10000; // # of bins for creating histogram (background bins)
        private int nBins; // # of bins for result
        private boolean autoShrink = false; // default: false
        private double[] quantiles; // for reset

        @AggregationType(estimable = true)
        static final class BuildBinsAggregationBuffer extends AbstractAggregationBuffer {
            boolean autoShrink;
            NumericHistogram histogram; // histogram used for quantile approximation
            double[] quantiles; // the quantiles requested

            BuildBinsAggregationBuffer() {}

            @Override
            public int estimate() {
                return (histogram != null ? histogram.lengthFor() : 0) // histogram
                        + 20 + 8 * (quantiles != null ? quantiles.length : 0) // quantiles
                        + 4; // autoShrink
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                weightOI = HiveUtils.asDoubleCompatibleOI(OIs[0]);

                // set const values
                nBins = HiveUtils.getConstInt(OIs[1]);
                if (OIs.length == 3) {
                    autoShrink = HiveUtils.getConstBoolean(OIs[2]);
                }

                // check value of `num_of_bins`
                if (nBins < 2) {
                    throw new UDFArgumentException(
                        "Only greater than or equal to 2 is accepted but " + nBins
                                + " was passed as `num_of_bins`.");
                }

                quantiles = getQuantiles();
            } else {
                structOI = (StructObjectInspector) OIs[0];
                autoShrinkField = structOI.getStructFieldRef("autoShrink");
                histogramField = structOI.getStructFieldRef("histogram");
                quantilesField = structOI.getStructFieldRef("quantiles");
                autoShrinkOI = (WritableBooleanObjectInspector) autoShrinkField.getFieldObjectInspector();
                histogramOI = (StandardListObjectInspector) histogramField.getFieldObjectInspector();
                quantilesOI = (StandardListObjectInspector) quantilesField.getFieldObjectInspector();
                histogramElOI = (WritableDoubleObjectInspector) histogramOI.getListElementObjectInspector();
                quantileOI = (WritableDoubleObjectInspector) quantilesOI.getListElementObjectInspector();
            }

            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
                fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

                return ObjectInspectorFactory.getStandardStructObjectInspector(
                    Arrays.asList("autoShrink", "histogram", "quantiles"), fieldOIs);
            } else {
                return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            }
        }

        private double[] getQuantiles() throws HiveException {
            final int nQuantiles = nBins - 1;
            final double[] result = new double[nQuantiles];
            for (int i = 0; i < nQuantiles; i++) {
                result[i] = ((double) (i + 1)) / (nQuantiles + 1);
            }
            return result;
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            final BuildBinsAggregationBuffer myAgg = new BuildBinsAggregationBuffer();
            myAgg.histogram = new NumericHistogram();
            reset(myAgg);
            return myAgg;
        }

        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            final BuildBinsAggregationBuffer myAgg = (BuildBinsAggregationBuffer) agg;
            myAgg.autoShrink = autoShrink;
            myAgg.histogram.reset();
            myAgg.histogram.allocate(nBGBins);
            myAgg.quantiles = quantiles;
        }

        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg,
                Object[] parameters) throws HiveException {
            Preconditions.checkArgument(parameters.length == 2 || parameters.length == 3);

            if (parameters[0] == null || parameters[1] == null) {
                return;
            }
            final BuildBinsAggregationBuffer myAgg = (BuildBinsAggregationBuffer) agg;

            // Get and process the current datum
            myAgg.histogram.add(PrimitiveObjectInspectorUtils.getDouble(parameters[0], weightOI));
        }

        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object other)
                throws HiveException {
            if (other == null) {
                return;
            }

            final BuildBinsAggregationBuffer myAgg = (BuildBinsAggregationBuffer) agg;

            myAgg.autoShrink = autoShrinkOI.get(structOI.getStructFieldData(other, autoShrinkField));

            final List<?> histogram = ((LazyBinaryArray) structOI.getStructFieldData(other,
                histogramField)).getList();
            myAgg.histogram.merge(histogram, histogramElOI);

            final double[] quantiles = HiveUtils.asDoubleArray(
                structOI.getStructFieldData(other, quantilesField), quantilesOI, quantileOI);
            if (quantiles != null && quantiles.length > 0) {
                myAgg.quantiles = quantiles;
            }
        }

        @SuppressWarnings("serial")
        @Override
        public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            final BuildBinsAggregationBuffer myAgg = (BuildBinsAggregationBuffer) agg;
            final Object[] partialResult = new Object[3];
            partialResult[0] = new BooleanWritable(myAgg.autoShrink);
            partialResult[1] = myAgg.histogram.serialize();
            partialResult[2] = (myAgg.quantiles != null) ? WritableUtils.toWritableList(myAgg.quantiles)
                    : Collections.singletonList(new DoubleWritable(0));
            return partialResult;
        }

        @Override
        public Object terminate(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            final BuildBinsAggregationBuffer myAgg = (BuildBinsAggregationBuffer) agg;

            if (myAgg.histogram.getUsedBins() < 1) { // SQL standard - return null for zero elements
                return null;
            } else {
                Preconditions.checkNotNull(myAgg.quantiles);

                final List<DoubleWritable> result = new ArrayList<DoubleWritable>();

                double prev = Double.NEGATIVE_INFINITY;

                result.add(new DoubleWritable(Double.NEGATIVE_INFINITY));
                for (int i = 0; i < myAgg.quantiles.length; i++) {
                    final double val = myAgg.histogram.quantile(myAgg.quantiles[i]);

                    // check duplication
                    if (prev == val) {
                        if (!myAgg.autoShrink) {
                            throw new HiveException(
                                "Quantiles were repeated even though `auto_shrink` is false."
                                        + " Reduce `num_of_bins` or enable `auto_shrink`.");
                        } // else: skip duplicated quantile
                    } else {
                        result.add(new DoubleWritable(val));
                        prev = val;
                    }
                }
                result.add(new DoubleWritable(Double.POSITIVE_INFINITY));

                return result;
            }
        }
    }
}
