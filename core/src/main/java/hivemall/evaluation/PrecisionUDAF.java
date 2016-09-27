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
package hivemall.evaluation;

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

@Description(
        name = "precision",
        value = "_FUNC_(array rankItems, array correctItems [, const int recommendSize = rankItems.size])"
                + " - Returns Precision")
public final class PrecisionUDAF extends AbstractGenericUDAFResolver {

    // prevent instantiation
    private PrecisionUDAF() {}

    @Override
    public GenericUDAFEvaluator getEvaluator(@Nonnull TypeInfo[] typeInfo) throws SemanticException {
        if (typeInfo.length != 2 && typeInfo.length != 3) {
            throw new UDFArgumentTypeException(typeInfo.length - 1,
                "_FUNC_ takes two or three arguments");
        }

        ListTypeInfo arg1type = HiveUtils.asListTypeInfo(typeInfo[0]);
        if (!HiveUtils.isPrimitiveTypeInfo(arg1type.getListElementTypeInfo())
                && !HiveUtils.isStructTypeInfo(arg1type.getListElementTypeInfo())) {
            throw new UDFArgumentTypeException(0,
                "The first argument `array rankItems` is invalid form: " + typeInfo[0]);
        }
        ListTypeInfo arg2type = HiveUtils.asListTypeInfo(typeInfo[1]);
        if (!HiveUtils.isPrimitiveTypeInfo(arg2type.getListElementTypeInfo())) {
            throw new UDFArgumentTypeException(1,
                "The first argument `array rankItems` is invalid form: " + typeInfo[1]);
        }

        return new Evaluator();
    }

    public static class Evaluator extends GenericUDAFEvaluator {

        private ListObjectInspector recommendListOI;
        private ListObjectInspector truthListOI;
        private WritableIntObjectInspector recommendSizeOI;

        private StructObjectInspector internalMergeOI;
        private StructField countField;
        private StructField sumField;

        public Evaluator() {}

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 2 || parameters.length == 3) : parameters.length;
            super.init(mode, parameters);

            // initialize input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                this.recommendListOI = (ListObjectInspector) parameters[0];
                this.truthListOI = (ListObjectInspector) parameters[1];
                if (parameters.length == 3) {
                    this.recommendSizeOI = (WritableIntObjectInspector) parameters[2];
                }
            } else {// from partial aggregation
                StructObjectInspector soi = (StructObjectInspector) parameters[0];
                this.internalMergeOI = soi;
                this.countField = soi.getStructFieldRef("count");
                this.sumField = soi.getStructFieldRef("sum");
            }

            // initialize output
            final ObjectInspector outputOI;
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// terminatePartial
                outputOI = internalMergeOI();
            } else {// terminate
                outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
            return outputOI;
        }

        private static StructObjectInspector internalMergeOI() {
            ArrayList<String> fieldNames = new ArrayList<String>();
            ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

            fieldNames.add("sum");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            fieldNames.add("count");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);

            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        @Override
        public PrecisionAggregationBuffer getNewAggregationBuffer() throws HiveException {
            PrecisionAggregationBuffer myAggr = new PrecisionAggregationBuffer();
            reset(myAggr);
            return myAggr;
        }

        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            PrecisionAggregationBuffer myAggr = (PrecisionAggregationBuffer) agg;
            myAggr.reset();
        }

        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg,
                Object[] parameters) throws HiveException {
            PrecisionAggregationBuffer myAggr = (PrecisionAggregationBuffer) agg;

            List<?> recommendList = recommendListOI.getList(parameters[0]);
            if (recommendList == null) {
                recommendList = Collections.emptyList();
            }
            List<?> truthList = truthListOI.getList(parameters[1]);
            if (truthList == null) {
                return;
            }

            int recommendSize = recommendList.size();
            if (parameters.length == 3) {
                recommendSize = recommendSizeOI.get(parameters[2]);
            }
            if (recommendSize < 0 || recommendSize > recommendList.size()) {
                throw new UDFArgumentException(
                    "The third argument `int recommendSize` must be in [0, " + recommendList.size()
                            + "]");
            }

            myAggr.iterate(recommendList, truthList, recommendSize);
        }

        @Override
        public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            PrecisionAggregationBuffer myAggr = (PrecisionAggregationBuffer) agg;

            Object[] partialResult = new Object[2];
            partialResult[0] = new DoubleWritable(myAggr.sum);
            partialResult[1] = new LongWritable(myAggr.count);
            return partialResult;
        }

        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial == null) {
                return;
            }

            Object sumObj = internalMergeOI.getStructFieldData(partial, sumField);
            Object countObj = internalMergeOI.getStructFieldData(partial, countField);
            double sum = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector.get(sumObj);
            long count = PrimitiveObjectInspectorFactory.writableLongObjectInspector.get(countObj);

            PrecisionAggregationBuffer myAggr = (PrecisionAggregationBuffer) agg;
            myAggr.merge(sum, count);
        }

        @Override
        public DoubleWritable terminate(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            PrecisionAggregationBuffer myAggr = (PrecisionAggregationBuffer) agg;
            double result = myAggr.get();
            return new DoubleWritable(result);
        }

    }

    public static class PrecisionAggregationBuffer extends AbstractAggregationBuffer {

        double sum;
        long count;

        public PrecisionAggregationBuffer() {
            super();
        }

        void reset() {
            this.sum = 0.d;
            this.count = 0;
        }

        void merge(double o_sum, long o_count) {
            sum += o_sum;
            count += o_count;
        }

        double get() {
            if (count == 0) {
                return 0.d;
            }
            return sum / count;
        }

        void iterate(@Nonnull List<?> recommendList, @Nonnull List<?> truthList,
                @Nonnull int recommendSize) {
            sum += BinaryResponsesMeasures.Precision(recommendList, truthList, recommendSize);
            count++;
        }
    }

}
