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
package hivemall.fm;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;

import java.util.ArrayList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(
        name = "fm_predict",
        value = "_FUNC_(Float Wj, array<float> Vjf, float Xj) - Returns a prediction value in Double")
public final class FMPredictGenericUDAF extends AbstractGenericUDAFResolver {

    private FMPredictGenericUDAF() {}

    @Override
    public Evaluator getEvaluator(TypeInfo[] typeInfo) throws SemanticException {
        if (typeInfo.length != 3) {
            throw new UDFArgumentLengthException(
                "Expected argument length is 3 but given argument length was " + typeInfo.length);
        }
        if (!HiveUtils.isNumberTypeInfo(typeInfo[0])) {
            throw new UDFArgumentTypeException(0,
                "Number type is expected for the first argument Wj: " + typeInfo[0].getTypeName());
        }
        if (typeInfo[1].getCategory() != Category.LIST) {
            throw new UDFArgumentTypeException(1,
                "List type is expected for the second argument Vjf: " + typeInfo[1].getTypeName());
        }
        ListTypeInfo typeInfo1 = (ListTypeInfo) typeInfo[1];
        if (!HiveUtils.isNumberTypeInfo(typeInfo1.getListElementTypeInfo())) {
            throw new UDFArgumentTypeException(1,
                "Number type is expected for the element type of list Vjf: "
                        + typeInfo1.getTypeName());
        }
        if (!HiveUtils.isNumberTypeInfo(typeInfo[2])) {
            throw new UDFArgumentTypeException(2,
                "Number type is expected for the third argument Xj: " + typeInfo[2].getTypeName());
        }
        return new Evaluator();
    }

    public static class Evaluator extends GenericUDAFEvaluator {

        // input OI
        private PrimitiveObjectInspector wOI;
        private ListObjectInspector vOI;
        private PrimitiveObjectInspector vElemOI;
        private PrimitiveObjectInspector xOI;

        // merge OI
        private StructObjectInspector internalMergeOI;
        private StructField retField, sumVjXjField, sumV2X2Field;
        private WritableDoubleObjectInspector retOI;
        private StandardListObjectInspector sumVjXjOI, sumV2X2OI;

        public Evaluator() {}

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 3);
            super.init(mode, parameters);

            // initialize input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                this.wOI = HiveUtils.asDoubleCompatibleOI(parameters[0]);
                this.vOI = HiveUtils.asListOI(parameters[1]);
                this.vElemOI = HiveUtils.asDoubleCompatibleOI(vOI.getListElementObjectInspector());
                this.xOI = HiveUtils.asDoubleCompatibleOI(parameters[2]);
            } else {// from partial aggregation
                StructObjectInspector soi = (StructObjectInspector) parameters[0];
                this.internalMergeOI = soi;
                this.retField = soi.getStructFieldRef("ret");
                this.sumVjXjField = soi.getStructFieldRef("sumVjXj");
                this.sumV2X2Field = soi.getStructFieldRef("sumV2X2");
                this.retOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
                this.sumVjXjOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                this.sumV2X2OI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            }

            // initialize output
            final ObjectInspector outputOI;
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// terminatePartial
                outputOI = internalMergeOI();
            } else {
                outputOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            }
            return outputOI;
        }

        private static StructObjectInspector internalMergeOI() {
            ArrayList<String> fieldNames = new ArrayList<String>();
            ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

            fieldNames.add("ret");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            fieldNames.add("sumVjXj");
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
            fieldNames.add("sumV2X2");
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        @Override
        public FMPredictAggregationBuffer getNewAggregationBuffer() throws HiveException {
            FMPredictAggregationBuffer buf = new FMPredictAggregationBuffer();
            buf.reset();
            return buf;
        }

        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            FMPredictAggregationBuffer buf = (FMPredictAggregationBuffer) agg;
            buf.reset();
        }

        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg,
                Object[] parameters) throws HiveException {
            if (parameters[0] == null) {
                return;
            }
            FMPredictAggregationBuffer buf = (FMPredictAggregationBuffer) agg;

            double w = PrimitiveObjectInspectorUtils.getDouble(parameters[0], wOI);
            if (parameters[1] == null || /* for TD */vOI.getListLength(parameters[1]) == 0) {// Vif was null
                buf.iterate(w);
            } else {
                if (parameters[2] == null) {
                    throw new UDFArgumentException("The third argument Xj must not be null");
                }
                double x = PrimitiveObjectInspectorUtils.getDouble(parameters[2], xOI);
                buf.iterate(w, x, parameters[1], vOI, vElemOI);
            }
        }

        @Override
        public Object terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            FMPredictAggregationBuffer buf = (FMPredictAggregationBuffer) agg;

            final Object[] partialResult = new Object[3];
            partialResult[0] = new DoubleWritable(buf.ret);
            if (buf.sumVjXj != null) {
                partialResult[1] = WritableUtils.toWritableList(buf.sumVjXj);
                partialResult[2] = WritableUtils.toWritableList(buf.sumV2X2);
            }
            return partialResult;
        }

        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial == null) {
                return;
            }
            FMPredictAggregationBuffer buf = (FMPredictAggregationBuffer) agg;

            Object o1 = internalMergeOI.getStructFieldData(partial, retField);
            double ret = retOI.get(o1);

            Object sumVjXj = internalMergeOI.getStructFieldData(partial, sumVjXjField);
            Object sumV2X2 = internalMergeOI.getStructFieldData(partial, sumV2X2Field);

            // --------------------------------------------------------------
            // [workaround]
            // java.lang.ClassCastException: org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray
            // cannot be cast to [Ljava.lang.Object;
            if (sumVjXj instanceof LazyBinaryArray) {
                sumVjXj = ((LazyBinaryArray) sumVjXj).getList();
            }
            if (sumV2X2 instanceof LazyBinaryArray) {
                sumV2X2 = ((LazyBinaryArray) sumV2X2).getList();
            }
            // --------------------------------------------------------------

            buf.merge(ret, sumVjXj, sumV2X2, sumVjXjOI, sumV2X2OI);
        }

        @Override
        public DoubleWritable terminate(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            FMPredictAggregationBuffer buf = (FMPredictAggregationBuffer) agg;
            double predict = buf.getPrediction();
            return new DoubleWritable(predict);
        }

    }

    public static class FMPredictAggregationBuffer extends AbstractAggregationBuffer {

        private double ret;
        private double[] sumVjXj;
        private double[] sumV2X2;

        FMPredictAggregationBuffer() {
            super();
        }

        void reset() {
            this.ret = 0.d;
            this.sumVjXj = null;
            this.sumV2X2 = null;
        }

        void iterate(double Wj) {
            this.ret += Wj;
        }

        void iterate(final double Wj, final double Xj, @Nonnull final Object Vif,
                @Nonnull final ListObjectInspector vOI,
                @Nonnull final PrimitiveObjectInspector vElemOI) throws HiveException {
            this.ret += (Wj * Xj);

            final int factors = vOI.getListLength(Vif);
            if (factors < 1) {
                throw new HiveException("# of Factor should be more than 0: " + factors);
            }

            if (sumVjXj == null) {
                this.sumVjXj = new double[factors];
                this.sumV2X2 = new double[factors];
            } else if (sumVjXj.length != factors) {
                throw new HiveException("Mismatch in the number of factors");
            }

            for (int f = 0; f < factors; f++) {
                Object o = vOI.getListElement(Vif, f);
                if (o == null) {
                    throw new HiveException("Vj" + f + " should not be null");
                }
                double v = PrimitiveObjectInspectorUtils.getDouble(o, vElemOI);
                double vx = v * Xj;

                sumVjXj[f] += vx;
                sumV2X2[f] += (vx * vx);
            }
        }

        void merge(final double o_ret, @Nullable final Object o_sumVjXj,
                @Nullable final Object o_sumV2X2,
                @Nonnull final StandardListObjectInspector sumVjXjOI,
                @Nonnull final StandardListObjectInspector sumV2X2OI) throws HiveException {
            this.ret += o_ret;
            if (o_sumVjXj == null) {
                return;
            }

            if (o_sumV2X2 == null) {//sanity check
                throw new HiveException("o_sumV2X2 should not be null");
            }

            final int factors = sumVjXjOI.getListLength(o_sumVjXj);
            if (sumVjXj == null) {
                this.sumVjXj = new double[factors];
                this.sumV2X2 = new double[factors];
            } else if (sumVjXj.length != factors) {//sanity check
                throw new HiveException("Mismatch in the number of factors");
            }

            final WritableDoubleObjectInspector doubleOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            for (int f = 0; f < factors; f++) {
                Object o1 = sumVjXjOI.getListElement(o_sumVjXj, f);
                Object o2 = sumV2X2OI.getListElement(o_sumV2X2, f);
                double d1 = doubleOI.get(o1);
                double d2 = doubleOI.get(o2);
                sumVjXj[f] += d1;
                sumV2X2[f] += d2;
            }
        }

        double getPrediction() {
            double predict = this.ret;
            if (sumVjXj != null) {
                final int factors = sumVjXj.length;
                for (int f = 0; f < factors; f++) {
                    double d1 = sumVjXj[f];
                    double d2 = sumV2X2[f];
                    predict += 0.5d * (d1 * d1 - d2);
                }
            }
            return predict;
        }
    }

}
