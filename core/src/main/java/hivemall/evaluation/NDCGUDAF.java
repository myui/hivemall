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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

@Description(name = "ndcg", value = "_FUNC_(array rankItems, array correctItems)"
        + " - Returns nDCG")
public final class NDCGUDAF extends AbstractGenericUDAFResolver {

    // prevent instantiation
    private NDCGUDAF() {}

    @Override
    public GenericUDAFEvaluator getEvaluator(@Nonnull TypeInfo[] typeInfo) throws SemanticException {
        if (typeInfo.length != 2) {
            throw new UDFArgumentTypeException(typeInfo.length - 1, "_FUNC_ takes two arguments");
        }
        ListTypeInfo arg1type = HiveUtils.asListTypeInfo(typeInfo[0]);
        if (!HiveUtils.isPrimitiveTypeInfo(arg1type.getListElementTypeInfo())) {
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

        private ListObjectInspector rankedListOI;
        private ListObjectInspector correctListOI;

        private StructObjectInspector internalMergeOI;
        private StructField countField;
        private StructField sumField;

        public Evaluator() {}

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 2) : parameters.length;
            super.init(mode, parameters);

            // initialize input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                this.rankedListOI = (ListObjectInspector) parameters[0];
                this.correctListOI = (ListObjectInspector) parameters[1];
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
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer myAggr = new NDCGAggregationBuffer();
            reset(myAggr);
            return myAggr;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            NDCGAggregationBuffer myAggr = (NDCGAggregationBuffer) agg;
            myAggr.reset();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            NDCGAggregationBuffer myAggr = (NDCGAggregationBuffer) agg;

            List<?> rankedList = rankedListOI.getList(parameters[0]);
            if (rankedList == null) {
                rankedList = Collections.emptyList();
            }
            List<?> correctList = correctListOI.getList(parameters[1]);
            if (correctList == null) {
                return;
            }

            myAggr.iterate(rankedList, correctList);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            NDCGAggregationBuffer myAggr = (NDCGAggregationBuffer) agg;

            Object[] partialResult = new Object[2];
            partialResult[0] = new DoubleWritable(myAggr.sum);
            partialResult[1] = new LongWritable(myAggr.count);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            Object sumObj = internalMergeOI.getStructFieldData(partial, sumField);
            Object countObj = internalMergeOI.getStructFieldData(partial, countField);
            double sum = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector.get(sumObj);
            long count = PrimitiveObjectInspectorFactory.writableLongObjectInspector.get(countObj);

            NDCGAggregationBuffer myAggr = (NDCGAggregationBuffer) agg;
            myAggr.merge(sum, count);
        }

        @Override
        public DoubleWritable terminate(AggregationBuffer agg) throws HiveException {
            NDCGAggregationBuffer myAggr = (NDCGAggregationBuffer) agg;
            double result = myAggr.get();
            return new DoubleWritable(result);
        }

    }

    public static class NDCGAggregationBuffer implements AggregationBuffer {

        double sum;
        long count;

        public NDCGAggregationBuffer() {}

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

        void iterate(@Nonnull List<?> rankedList, @Nonnull List<?> correctList) {
            sum += BinaryResponsesMeasures.nDCG(rankedList, correctList);
            count++;
        }

    }

}
