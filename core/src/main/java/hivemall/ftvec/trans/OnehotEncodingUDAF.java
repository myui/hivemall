/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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
package hivemall.ftvec.trans;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Identifier;
import hivemall.utils.lang.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

@Description(name = "onehot_encoding",
        value = "_FUNC_(PRIMITIVE feature, ...) - Compute onehot encoded label for each feature")
@UDFType(deterministic = true, stateful = true)
public final class OnehotEncodingUDAF extends AbstractGenericUDAFResolver {

    public OnehotEncodingUDAF() {
        super();
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(@Nonnull TypeInfo[] argTypes) throws SemanticException {
        final int numFeatures = argTypes.length;
        if (numFeatures == 0) {
            throw new UDFArgumentException("_FUNC_ requires at least 1 argument");
        }
        for (int i = 0; i < numFeatures; i++) {
            if (argTypes[i] == null) {
                throw new UDFArgumentTypeException(i,
                    "Null type is found. Only primitive type arguments are accepted.");
            }
            if (argTypes[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                    "Only primitive type arguments are accepted but " + argTypes[i].getTypeName()
                            + " was passed as parameter 1.");
            }
        }

        return new GenericUDAFOnehotEncodingEvaluator();
    }

    public static final class GenericUDAFOnehotEncodingEvaluator extends GenericUDAFEvaluator {

        // input OI
        private PrimitiveObjectInspector[] inputElemOIs;
        // merge input OI
        private StructObjectInspector mergeOI;
        private StructField[] fields;
        private ListObjectInspector[] fieldOIs;

        public GenericUDAFOnehotEncodingEvaluator() {}

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] argOIs) throws HiveException {
            super.init(m, argOIs);

            // initialize input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {// from original data
                this.inputElemOIs = new PrimitiveObjectInspector[argOIs.length];
                for (int i = 0; i < argOIs.length; i++) {
                    inputElemOIs[i] = HiveUtils.asPrimitiveObjectInspector(argOIs[i]);
                }
            } else {// from partial aggregation
                Preconditions.checkArgument(argOIs.length == 1);
                this.mergeOI = HiveUtils.asStructOI(argOIs[0]);
                final int numFields = mergeOI.getAllStructFieldRefs().size();
                this.fields = new StructField[numFields];
                this.fieldOIs = new ListObjectInspector[numFields];
                this.inputElemOIs = new PrimitiveObjectInspector[numFields];
                for (int i = 0; i < numFields; i++) {
                    StructField field = mergeOI.getStructFieldRef("f" + String.valueOf(i));
                    fields[i] = field;
                    ListObjectInspector fieldOI = HiveUtils.asListOI(field.getFieldObjectInspector());
                    fieldOIs[i] = fieldOI;
                    inputElemOIs[i] = HiveUtils.asPrimitiveObjectInspector(fieldOI.getListElementObjectInspector());
                }
            }

            // initialize output
            final ObjectInspector outputOI;
            switch (m) {
                case PARTIAL1:// from original data to partial aggregation data                    
                    outputOI = internalMergeOutputOI(inputElemOIs);
                    break;
                case PARTIAL2:// from partial aggregation data to partial aggregation data
                    outputOI = internalMergeOutputOI(inputElemOIs);
                    break;
                case COMPLETE:// from original data directly to full aggregation
                    outputOI = terminalOutputOI(inputElemOIs);
                    break;
                case FINAL: // from partial aggregation to full aggregation
                    outputOI = terminalOutputOI(inputElemOIs);
                    break;
                default:
                    throw new IllegalStateException("Illegal mode: " + m);
            }
            return outputOI;
        }

        @Nonnull
        private static StructObjectInspector internalMergeOutputOI(
                @CheckForNull PrimitiveObjectInspector[] inputOIs) throws UDFArgumentException {
            Preconditions.checkNotNull(inputOIs);

            final int numOIs = inputOIs.length;
            final List<String> fieldNames = new ArrayList<String>(numOIs);
            final List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numOIs);
            for (int i = 0; i < numOIs; i++) {
                fieldNames.add("f" + String.valueOf(i));
                ObjectInspector elemOI = ObjectInspectorUtils.getStandardObjectInspector(
                    inputOIs[i], ObjectInspectorCopyOption.WRITABLE);
                ListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(elemOI);
                fieldOIs.add(listOI);
            }
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        @Nonnull
        private static StructObjectInspector terminalOutputOI(
                @CheckForNull PrimitiveObjectInspector[] inputOIs) {
            Preconditions.checkNotNull(inputOIs);
            Preconditions.checkArgument(inputOIs.length >= 1, inputOIs.length);

            final List<String> fieldNames = new ArrayList<>(inputOIs.length);
            final List<ObjectInspector> fieldOIs = new ArrayList<>(inputOIs.length);
            for (int i = 0; i < inputOIs.length; i++) {
                fieldNames.add("f" + String.valueOf(i + 1));
                ObjectInspector keyOI = ObjectInspectorUtils.getStandardObjectInspector(
                    inputOIs[i], ObjectInspectorCopyOption.WRITABLE);
                MapObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                    keyOI, PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                fieldOIs.add(mapOI);
            }
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        @SuppressWarnings("deprecation")
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            EncodingBuffer buf = new EncodingBuffer();
            reset(buf);
            return buf;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            EncodingBuffer buf = (EncodingBuffer) aggregationBuffer;
            buf.reset();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] parameters)
                throws HiveException {
            Preconditions.checkNotNull(inputElemOIs);

            EncodingBuffer buf = (EncodingBuffer) aggregationBuffer;
            buf.iterate(parameters, inputElemOIs);
        }

        @SuppressWarnings("deprecation")
        @Override
        public Object[] terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            EncodingBuffer buf = (EncodingBuffer) aggregationBuffer;
            return buf.partial();
        }

        @SuppressWarnings("deprecation")
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object partial) throws HiveException {
            if (partial == null) {
                return;
            }

            EncodingBuffer buf = (EncodingBuffer) aggregationBuffer;
            buf.merge(partial, mergeOI, fields, fieldOIs);
        }

        @SuppressWarnings("deprecation")
        @Override
        public Object[] terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            EncodingBuffer buf = (EncodingBuffer) aggregationBuffer;
            return buf.terminate();
        }
    }

    public static final class EncodingBuffer extends AbstractAggregationBuffer {

        @Nullable
        private Identifier<Writable>[] identifiers;

        public EncodingBuffer() {}

        void reset() {
            this.identifiers = null;
        }

        @SuppressWarnings("unchecked")
        void iterate(@Nonnull final Object[] args,
                @Nonnull final PrimitiveObjectInspector[] inputOIs) throws HiveException {
            Preconditions.checkArgument(args.length == inputOIs.length);

            final int length = args.length;
            if (identifiers == null) {
                this.identifiers = new Identifier[length];
                for (int i = 0; i < length; i++) {
                    identifiers[i] = new Identifier<>(1);
                }
            }

            for (int i = 0; i < length; i++) {
                Object arg = args[i];
                if (arg == null) {
                    continue;
                }
                Writable writable = WritableUtils.copyToWritable(arg, inputOIs[i]);
                identifiers[i].put(writable);
            }
        }

        @Nullable
        Object[] partial() throws HiveException {
            if (identifiers == null) {
                return null;
            }

            final int length = identifiers.length;
            final Object[] partial = new Object[length];
            for (int i = 0; i < length; i++) {
                Set<Writable> id = identifiers[i].getMap().keySet();
                final List<Writable> list = new ArrayList<Writable>(id.size());
                for (Writable e : id) {
                    Preconditions.checkNotNull(e);
                    list.add(e);
                }
                partial[i] = list;
            }
            return partial;
        }

        @SuppressWarnings("unchecked")
        void merge(@Nonnull final Object partial, @Nonnull final StructObjectInspector mergeOI,
                @Nonnull final StructField[] fields, @Nonnull final ListObjectInspector[] fieldOIs) {
            Preconditions.checkArgument(fields.length == fieldOIs.length);

            final int numFields = fieldOIs.length;
            if (identifiers == null) {
                this.identifiers = new Identifier[numFields];
            }
            Preconditions.checkArgument(fields.length == identifiers.length);

            for (int i = 0; i < numFields; i++) {
                Identifier<Writable> id = identifiers[i];
                if (id == null) {
                    id = new Identifier<>(1);
                    identifiers[i] = id;
                }
                final Object fieldData = mergeOI.getStructFieldData(partial, fields[i]);
                final ListObjectInspector fieldOI = fieldOIs[i];
                for (int j = 0, size = fieldOI.getListLength(fieldData); j < size; j++) {
                    Object o = fieldOI.getListElement(fieldData, j);
                    Preconditions.checkNotNull(o);
                    id.valueOf((Writable) o);
                }
            }
        }

        @Nullable
        Object[] terminate() {
            if (identifiers == null) {
                return null;
            }
            final Object[] ret = new Object[identifiers.length];
            int max = 0;
            for (int i = 0; i < identifiers.length; i++) {
                final Map<Writable, Integer> m = identifiers[i].getMap();
                if (max != 0) {
                    for (Map.Entry<Writable, Integer> e : m.entrySet()) {
                        int original = e.getValue().intValue();
                        e.setValue(Integer.valueOf(max + original));
                    }
                }
                ret[i] = m;
                max += m.size();
            }
            return ret;
        }

    }
}
