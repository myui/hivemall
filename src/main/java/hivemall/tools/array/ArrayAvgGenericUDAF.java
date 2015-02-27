/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.tools.array;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public final class ArrayAvgGenericUDAF extends AbstractGenericUDAFResolver {

    private ArrayAvgGenericUDAF() {}//prevent instantiation

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfo) throws SemanticException {
        if(typeInfo.length != 1) {
            throw new UDFArgumentTypeException(typeInfo.length - 1, "One argument is expected, taking an array as an argument");
        }
        if(!typeInfo[0].getCategory().equals(Category.LIST)) {
            throw new UDFArgumentTypeException(typeInfo.length - 1, "One argument is expected, taking an array as an argument");
        }
        return new Evaluator();
    }

    public static class Evaluator extends GenericUDAFEvaluator {

        private ListObjectInspector inputListOI;
        private PrimitiveObjectInspector inputListElemOI;

        private StructObjectInspector internalMergeOI;
        private StructField sizeField, sumField, countField;
        private WritableIntObjectInspector sizeOI;
        private StandardListObjectInspector sumOI;
        private StandardListObjectInspector countOI;

        public Evaluator() {}

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(mode, parameters);
            // initialize input
            if(mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                this.inputListOI = (ListObjectInspector) parameters[0];
                this.inputListElemOI = HiveUtils.asDoubleCompatibleOI(inputListOI.getListElementObjectInspector());
            } else {// from partial aggregation
                StructObjectInspector soi = (StructObjectInspector) parameters[0];
                this.internalMergeOI = soi;
                this.sizeField = soi.getStructFieldRef("size");
                this.sumField = soi.getStructFieldRef("sum");
                this.countField = soi.getStructFieldRef("count");
                this.sizeOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
                this.sumOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                this.countOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            }

            // initialize output
            final ObjectInspector outputOI;
            if(mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// terminatePartial
                outputOI = internalMergeOI();
            } else {// terminate
                outputOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
            }
            return outputOI;
        }

        private static StructObjectInspector internalMergeOI() {
            ArrayList<String> fieldNames = new ArrayList<String>();
            ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

            fieldNames.add("size");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            fieldNames.add("sum");
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
            fieldNames.add("count");
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector));

            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer aggr = new ArrayAvgAggregationBuffer();
            reset(aggr);
            return aggr;
        }

        @Override
        public void reset(AggregationBuffer aggr) throws HiveException {
            ArrayAvgAggregationBuffer myAggr = (ArrayAvgAggregationBuffer) aggr;
            myAggr.reset();
        }

        @Override
        public void iterate(AggregationBuffer aggr, Object[] parameters) throws HiveException {
            ArrayAvgAggregationBuffer myAggr = (ArrayAvgAggregationBuffer) aggr;

            Object tuple = parameters[0];
            if(tuple != null) {
                myAggr.doIterate(tuple, inputListOI, inputListElemOI);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggr) throws HiveException {
            ArrayAvgAggregationBuffer myAggr = (ArrayAvgAggregationBuffer) aggr;
            if(myAggr._size == -1) {
                return null;
            }

            Object[] partialResult = new Object[3];
            partialResult[0] = new IntWritable(myAggr._size);
            partialResult[1] = WritableUtils.toWritableList(myAggr._sum);
            partialResult[2] = WritableUtils.toWritableList(myAggr._count);

            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer aggr, Object partial) throws HiveException {
            if(partial != null) {
                ArrayAvgAggregationBuffer myAggr = (ArrayAvgAggregationBuffer) aggr;

                Object o1 = internalMergeOI.getStructFieldData(partial, sizeField);
                int size = sizeOI.get(o1);
                assert size != -1;

                Object sum = internalMergeOI.getStructFieldData(partial, sumField);
                Object count = internalMergeOI.getStructFieldData(partial, countField);

                // --------------------------------------------------------------
                // [workaround]
                // java.lang.ClassCastException: org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray cannot be cast to [Ljava.lang.Object;
                if(sum instanceof LazyBinaryArray) {
                    sum = ((LazyBinaryArray) sum).getList();
                }
                if(count instanceof LazyBinaryArray) {
                    count = ((LazyBinaryArray) count).getList();
                }
                // --------------------------------------------------------------

                myAggr.merge(size, sum, count, sumOI, countOI);
            }
        }

        @Override
        public List<FloatWritable> terminate(AggregationBuffer aggr) throws HiveException {
            ArrayAvgAggregationBuffer myAggr = (ArrayAvgAggregationBuffer) aggr;

            final int size = myAggr._size;
            if(size == -1) {
                return null;
            }

            final double[] sum = myAggr._sum;
            final long[] count = myAggr._count;

            final FloatWritable[] ary = new FloatWritable[size];
            for(int i = 0; i < size; i++) {
                long c = count[i];
                float avg = (c == 0) ? 0.f : (float) (sum[i] / c);
                ary[i] = new FloatWritable(avg);
            }
            return Arrays.asList(ary);
        }
    }

    public static class ArrayAvgAggregationBuffer implements AggregationBuffer {

        int _size;
        // note that primitive array cannot be serialized by JDK serializer
        double[] _sum;
        long[] _count;

        public ArrayAvgAggregationBuffer() {}

        void reset() {
            this._size = -1;
            this._sum = null;
            this._count = null;
        }

        void init(int size) throws HiveException {
            assert (size > 0) : size;
            this._size = size;
            this._sum = new double[size];
            this._count = new long[size];
        }

        void doIterate(@Nonnull final Object tuple, @Nonnull ListObjectInspector listOI, @Nonnull PrimitiveObjectInspector elemOI)
                throws HiveException {
            final int size = listOI.getListLength(tuple);
            if(_size == -1) {
                init(size);
            }
            if(size != _size) {// a corner case
                throw new HiveException("Mismatch in the number of elements at tuple: "
                        + tuple.toString());
            }
            final double[] sum = _sum;
            final long[] count = _count;
            for(int i = 0, len = size; i < len; i++) {
                Object o = listOI.getListElement(tuple, i);
                if(o != null) {
                    double v = PrimitiveObjectInspectorUtils.getDouble(o, elemOI);
                    sum[i] += v;
                    count[i] += 1L;
                }
            }
        }

        void merge(final int o_size, @Nonnull final Object o_sum, @Nonnull final Object o_count, @Nonnull final StandardListObjectInspector sumOI, @Nonnull final StandardListObjectInspector countOI)
                throws HiveException {
            final WritableDoubleObjectInspector sumElemOI = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
            final WritableLongObjectInspector countElemOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;

            if(o_size != _size) {
                if(_size == -1) {
                    init(o_size);
                } else {
                    throw new HiveException("Mismatch in the number of elements");
                }
            }
            final double[] sum = _sum;
            final long[] count = _count;
            for(int i = 0, len = _size; i < len; i++) {
                Object sum_e = sumOI.getListElement(o_sum, i);
                sum[i] += sumElemOI.get(sum_e);
                Object count_e = countOI.getListElement(o_count, i);
                count[i] += countElemOI.get(count_e);
            }
        }

    }

}
