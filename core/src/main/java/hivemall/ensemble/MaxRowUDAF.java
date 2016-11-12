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
package hivemall.ensemble;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

@Description(
        name = "maxrow",
        value = "_FUNC_(ANY compare, ...) - Returns a row that has maximum value in the 1st argument")
public final class MaxRowUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
        if (!ObjectInspectorUtils.compareSupported(oi)) {
            throw new UDFArgumentTypeException(0,
                "Cannot support comparison of map<> type or complex type containing map<>.");
        }
        return new GenericUDAFMaxRowEvaluator();
    }

    @UDFType(distinctLike = true)
    public static class GenericUDAFMaxRowEvaluator extends GenericUDAFEvaluator {

        StructObjectInspector inputStructOI;
        ObjectInspector[] inputOIs;
        ObjectInspector[] outputOIs;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);

            if (parameters.length == 1 && parameters[0] instanceof StructObjectInspector) {
                return initReduceSide((StructObjectInspector) parameters[0]);
            } else {
                return initMapSide(parameters);
            }
        }

        private ObjectInspector initMapSide(ObjectInspector[] parameters) throws HiveException {
            int length = parameters.length;
            this.inputOIs = parameters;
            this.outputOIs = new ObjectInspector[length];

            List<String> fieldNames = new ArrayList<String>(length);
            List<ObjectInspector> fieldOIs = Arrays.asList(outputOIs);
            for (int i = 0; i < length; i++) {
                fieldNames.add("col" + i);
                outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(parameters[i]);
            }

            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
        }

        private ObjectInspector initReduceSide(StructObjectInspector inputStructOI)
                throws HiveException {
            List<? extends StructField> fields = inputStructOI.getAllStructFieldRefs();
            int length = fields.size();
            this.inputStructOI = inputStructOI;
            this.inputOIs = new ObjectInspector[length];
            this.outputOIs = new ObjectInspector[length];

            for (int i = 0; i < length; i++) {
                StructField field = fields.get(i);
                ObjectInspector oi = field.getFieldObjectInspector();
                inputOIs[i] = oi;
                outputOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(oi);
            }

            return ObjectInspectorUtils.getStandardObjectInspector(inputStructOI);
        }

        static class MaxAgg extends AbstractAggregationBuffer {
            Object[] objects;

            MaxAgg() {
                super();
            }

            void reset() {
                this.objects = null;
            }
        }

        @Override
        public MaxAgg getNewAggregationBuffer() throws HiveException {
            MaxAgg maxagg = new MaxAgg();
            maxagg.reset();
            return maxagg;
        }

        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            MaxAgg maxagg = (MaxAgg) agg;
            maxagg.reset();
        }

        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg,
                Object[] parameters) throws HiveException {
            merge(agg, parameters);
        }

        @Override
        public List<Object> terminatePartial(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial)
                throws HiveException {
            if (partial == null) {
                return;
            }

            final MaxAgg maxagg = (MaxAgg) agg;

            final List<Object> otherObjects;
            if (partial instanceof Object[]) {
                otherObjects = Arrays.asList((Object[]) partial);
            } else if (partial instanceof LazyBinaryStruct) {
                otherObjects = ((LazyBinaryStruct) partial).getFieldsAsList();
            } else if (inputStructOI != null) {
                otherObjects = inputStructOI.getStructFieldsDataAsList(partial);
            } else {
                throw new HiveException("Invalid type: " + partial.getClass().getName());
            }

            boolean isMax = false;
            if (maxagg.objects == null) {
                isMax = true;
            } else {
                int cmp = ObjectInspectorUtils.compare(maxagg.objects[0], outputOIs[0],
                    otherObjects.get(0), inputOIs[0]);
                if (cmp < 0) {
                    isMax = true;
                }
            }

            if (isMax) {
                int length = otherObjects.size();
                maxagg.objects = new Object[length];
                for (int i = 0; i < length; i++) {
                    maxagg.objects[i] = ObjectInspectorUtils.copyToStandardObject(
                        otherObjects.get(i), inputOIs[i]);
                }
            }
        }

        @Override
        public List<Object> terminate(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            MaxAgg maxagg = (MaxAgg) agg;
            return Arrays.asList(maxagg.objects);
        }
    }
}
