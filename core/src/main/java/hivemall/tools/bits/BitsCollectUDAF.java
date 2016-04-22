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
package hivemall.tools.bits;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;

import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

@Description(name = "bits_collect",
        value = "_FUNC_(int|long x) - Returns a bitset in array<long>")
public final class BitsCollectUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfo) throws SemanticException {
        if (typeInfo.length != 1) {
            throw new UDFArgumentTypeException(typeInfo.length - 1,
                "Exactly one argument is expected");
        }
        if (!HiveUtils.isIntegerTypeInfo(typeInfo[0])) {
            throw new UDFArgumentTypeException(0, "_FUNC_(int|long x) is expected: " + typeInfo[0]);
        }
        return new Evaluator();
    }

    public static class Evaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private ListObjectInspector mergeOI;
        private PrimitiveObjectInspector mergeListElemOI;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] argOIs) throws HiveException {
            assert (argOIs.length == 1);
            super.init(mode, argOIs);

            // initialize input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                this.inputOI = HiveUtils.asLongCompatibleOI(argOIs[0]);
            } else {// from partial aggregation
                this.mergeOI = HiveUtils.asListOI(argOIs[0]);
                this.mergeListElemOI = HiveUtils.asPrimitiveObjectInspector(mergeOI.getListElementObjectInspector());
            }

            // initialize output
            final ObjectInspector outputOI;
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {// terminatePartial
                outputOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            } else {// terminate
                outputOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            }
            return outputOI;
        }

        static class ArrayAggregationBuffer implements AggregationBuffer {
            BitSet bitset;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void reset(AggregationBuffer aggr) throws HiveException {
            ((ArrayAggregationBuffer) aggr).bitset = new BitSet();
        }

        @Override
        public void iterate(AggregationBuffer aggr, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            Object arg = parameters[0];
            if (arg != null) {
                int index = PrimitiveObjectInspectorUtils.getInt(arg, inputOI);
                if (index < 0) {
                    throw new UDFArgumentException("Specified index SHOULD NOT be negative: "
                            + index);
                }
                ArrayAggregationBuffer agg = (ArrayAggregationBuffer) aggr;
                agg.bitset.set(index);
            }
        }

        @Override
        public List<LongWritable> terminatePartial(AggregationBuffer aggr) throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) aggr;
            long[] array = agg.bitset.toLongArray();
            if (agg.bitset == null || agg.bitset.isEmpty()) {
                return null;
            }
            return WritableUtils.toWritableList(array);
        }

        @Override
        public void merge(AggregationBuffer aggr, Object other) throws HiveException {
            if (other == null) {
                return;
            }
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) aggr;
            long[] longs = HiveUtils.asLongArray(other, mergeOI, mergeListElemOI);
            BitSet otherBitset = BitSet.valueOf(longs);
            agg.bitset.or(otherBitset);
        }

        @Override
        public List<LongWritable> terminate(AggregationBuffer aggr) throws HiveException {
            ArrayAggregationBuffer agg = (ArrayAggregationBuffer) aggr;
            long[] longs = agg.bitset.toLongArray();
            return WritableUtils.toWritableList(longs);
        }
    }
}
