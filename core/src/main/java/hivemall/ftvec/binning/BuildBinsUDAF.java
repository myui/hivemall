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

import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

@Description(
        name = "build_bins",
        value = "_FUNC_(int|bigint|float|double weight, const int num_of_bins[, const boolean auto_shrink = false])"
                + " - Return quantiles representing bins: array<double>")
public final class BuildBinsUDAF extends AbstractGenericUDAFResolver {
    private static final int idxOfCol = 0;
    private static final int idxOfNumOfBins = 1;
    private static final int idxOfAutoShrink = 2;

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        ObjectInspector[] OIs = info.getParameterObjectInspectors();

        if (OIs.length != 2 && OIs.length != 3)
            throw new UDFArgumentLengthException("Specify two or three arguments.");

        // check type of col
        if (OIs[idxOfCol].getCategory() != ObjectInspector.Category.PRIMITIVE)
            throw new UDFArgumentTypeException(idxOfCol,
                "Only primitive type arguments are accepted but " + OIs[idxOfCol].getTypeName()
                        + " was passed as `col`.");
        switch (((PrimitiveObjectInspector) OIs[idxOfCol]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
            case DECIMAL:
                break;
            default:
                throw new UDFArgumentTypeException(idxOfCol,
                    "Only numeric type arguments are accepted but " + OIs[idxOfCol].getTypeName()
                            + " was passed as `col`.");
        }

        // check type of num_of_bins
        if (OIs[idxOfNumOfBins].getCategory() != ObjectInspector.Category.PRIMITIVE)
            throw new UDFArgumentTypeException(idxOfNumOfBins,
                "Only primitive type arguments are accepted but " + OIs[idxOfCol].getTypeName()
                        + " was passed as `num_of_bins`.");
        switch (((PrimitiveObjectInspector) OIs[idxOfNumOfBins]).getPrimitiveCategory()) {
            case INT:
                break;
            default:
                throw new UDFArgumentTypeException(idxOfNumOfBins,
                    "Only int arguments are accepted but " + OIs[idxOfNumOfBins].getTypeName()
                            + " was passed as `num_of_bins`.");
        }

        if (OIs.length == 3) {
            // check type of auto_shrink
            if (OIs[idxOfAutoShrink].getCategory() != ObjectInspector.Category.PRIMITIVE)
                throw new UDFArgumentTypeException(idxOfAutoShrink,
                    "Only primitive type arguments are accepted but " + OIs[idxOfCol].getTypeName()
                            + " was passed as `auto_shrink`.");
            switch (((PrimitiveObjectInspector) OIs[idxOfAutoShrink]).getPrimitiveCategory()) {
                case BOOLEAN:
                    break;
                default:
                    throw new UDFArgumentTypeException(idxOfAutoShrink,
                        "Only boolean arguments are accepted but "
                                + OIs[idxOfAutoShrink].getTypeName()
                                + " was passed as `auto_shrink`.");
            }
        }

        return new BuildBinsUDAFEvaluator();
    }

    private static class BuildBinsUDAFEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector weightOI;
        private StructObjectInspector structOI;
        private StructField autoShrinkField, histogramField, quantilesField;
        private WritableBooleanObjectInspector autoShrinkOI;
        private StandardListObjectInspector histogramOI;
        private WritableDoubleObjectInspector histogramElOI;
        private StandardListObjectInspector quantilesOI;
        private WritableDoubleObjectInspector quantileOI;

        private double[] quantiles;

        private int nBGBins = 10000; // # of bins for creating histogram (background bins)
        private int nBins; // # of bins for result
        private boolean autoShrink;


        @AggregationType(estimable = true)
        static final class BuildBinsAggregationBuffer extends AbstractAggregationBuffer {
            boolean autoShrink;
            NumericHistogram histogram; // histogram used for quantile approximation
            double[] quantiles; // the quantiles requested

            BuildBinsAggregationBuffer() {}

            @Override
            public int estimate() {
                return histogram.lengthFor() // histogram
                        + 20 + 8 * quantiles.length // quantiles
                        + 4; // autoShrink
            }
        }

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] OIs) throws HiveException {
            super.init(mode, OIs);

            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                weightOI = (PrimitiveObjectInspector) OIs[idxOfCol];

                // set const values
                nBins = HiveUtils.getConstInt(OIs[idxOfNumOfBins]);
                if (OIs.length == 3)
                    autoShrink = HiveUtils.getConstBoolean(OIs[idxOfAutoShrink]);

                // check value of `num_of_bins`
                if (nBins < 2)
                    throw new UDFArgumentException(
                        "Only greater than or equal to 2 is accepted but " + nBins
                                + " was passed as `num_of_bins`.");

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
                ArrayList<String> fieldNames = new ArrayList<String>();
                ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
                fieldNames.add("autoShrink");
                fieldOIs.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
                fieldNames.add("histogram");
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
                fieldNames.add("quantiles");
                fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

                return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
            } else {
                return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            }
        }

        private double[] getQuantiles() throws HiveException {
            int nQuantiles = nBins - 1;
            double[] result = new double[nQuantiles];
            for (int i = 0; i < nQuantiles; i++)
                result[i] = ((double) (i + 1)) / (nQuantiles + 1);
            return result;
        }

        @Override
        public AbstractAggregationBuffer getNewAggregationBuffer() throws HiveException {
            BuildBinsAggregationBuffer result = new BuildBinsAggregationBuffer();
            result.histogram = new NumericHistogram();
            reset(result);
            return result;
        }

        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            BuildBinsAggregationBuffer result = (BuildBinsAggregationBuffer) agg;
            result.histogram.reset();
            result.quantiles = null;

            result.histogram.allocate(nBGBins);
            result.quantiles = quantiles;
            result.autoShrink = autoShrink;
        }

        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg,
                Object[] parameters) throws HiveException {
            assert (parameters.length == 2 || parameters.length == 3);
            if (parameters[0] == null || parameters[1] == null) {
                return;
            }
            BuildBinsAggregationBuffer myagg = (BuildBinsAggregationBuffer) agg;

            // Get and process the current datum
            double v = PrimitiveObjectInspectorUtils.getDouble(parameters[0], weightOI);
            myagg.histogram.add(v);
        }

        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object other)
                throws HiveException {
            if (other == null)
                return;

            BuildBinsAggregationBuffer myagg = (BuildBinsAggregationBuffer) agg;

            myagg.autoShrink = autoShrinkOI.get(structOI.getStructFieldData(other, autoShrinkField));

            List<?> histogram = ((LazyBinaryArray) structOI.getStructFieldData(other,
                histogramField)).getList();

            myagg.histogram.merge(histogram, histogramElOI);

            double[] quantiles = HiveUtils.asDoubleArray(
                structOI.getStructFieldData(other, quantilesField), quantilesOI, quantileOI);
            if (quantiles.length > 0)
                myagg.quantiles = quantiles;
        }

        @SuppressWarnings("serial")
        @Override
        public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            BuildBinsAggregationBuffer myagg = (BuildBinsAggregationBuffer) agg;
            Object[] partialResult = new Object[3];
            partialResult[0] = new BooleanWritable(myagg.autoShrink);
            partialResult[1] = myagg.histogram.serialize();
            partialResult[2] = (myagg.quantiles != null) ? WritableUtils.toWritableList(myagg.quantiles)
                    : new ArrayList<DoubleWritable>() {
                        {
                            add(new DoubleWritable(0));
                        }
                    };
            return partialResult;
        }

        @Override
        public Object terminate(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            BuildBinsAggregationBuffer myagg = (BuildBinsAggregationBuffer) agg;

            if (myagg.histogram.getUsedBins() < 1) { // SQL standard - return null for zero elements
                return null;
            } else {
                List<DoubleWritable> result = new ArrayList<DoubleWritable>();
                Preconditions.checkNotNull(myagg.quantiles);

                double prev = Double.NEGATIVE_INFINITY;

                result.add(new DoubleWritable(Double.NEGATIVE_INFINITY));
                for (int i = 0; i < myagg.quantiles.length; i++) {
                    double val = myagg.histogram.quantile(myagg.quantiles[i]);

                    // check duplication
                    if (prev == val) {
                        if (!myagg.autoShrink) {
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
