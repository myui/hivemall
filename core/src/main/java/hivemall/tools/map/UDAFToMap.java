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
package hivemall.tools.map;

import hivemall.utils.hadoop.HiveUtils;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Convert two aggregated columns into a key-value map.
 * 
 * The key must be a primitive type (int, boolean, float, string, ...) and the value may be a
 * primitive or a complex type (structs, maps, arrays).
 * 
 * @see https://cwiki.apache.org/Hive/genericudafcasestudy.html
 */
@Description(name = "to_map",
        value = "_FUNC_(key, value) - Convert two aggregated columns into a key-value map")
public class UDAFToMap extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] typeInfo) throws SemanticException {
        if (typeInfo.length != 2) {
            throw new UDFArgumentTypeException(typeInfo.length - 1,
                "Expecting exactly two arguments: " + typeInfo.length);
        }
        if (typeInfo[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "Only primitive type arguments are accepted for the key but "
                        + typeInfo[0].getTypeName() + " was passed as parameter 1.");
        }

        return new UDAFToMapEvaluator();
    }

    public static class UDAFToMapEvaluator extends GenericUDAFEvaluator {

        protected PrimitiveObjectInspector inputKeyOI;
        protected ObjectInspector inputValueOI;
        protected StandardMapObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] argOIs) throws HiveException {
            super.init(mode, argOIs);

            // initialize input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {// from original data
                inputKeyOI = HiveUtils.asPrimitiveObjectInspector(argOIs[0]);
                inputValueOI = argOIs[1];
            } else {// from partial aggregation
                internalMergeOI = (StandardMapObjectInspector) argOIs[0];
                inputKeyOI = HiveUtils.asPrimitiveObjectInspector(internalMergeOI.getMapKeyObjectInspector());
                inputValueOI = internalMergeOI.getMapValueObjectInspector();
            }

            return ObjectInspectorFactory.getStandardMapObjectInspector(
                ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI),
                ObjectInspectorUtils.getStandardObjectInspector(inputValueOI));
        }

        static class MapAggregationBuffer extends AbstractAggregationBuffer {
            Map<Object, Object> container;

            MapAggregationBuffer() {
                super();
            }
        }

        @Override
        public void reset(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            ((MapAggregationBuffer) agg).container = new HashMap<Object, Object>(64);
        }

        @Override
        public MapAggregationBuffer getNewAggregationBuffer() throws HiveException {
            MapAggregationBuffer ret = new MapAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void iterate(@SuppressWarnings("deprecation") AggregationBuffer agg,
                Object[] parameters) throws HiveException {
            assert (parameters.length == 2);
            Object key = parameters[0];
            Object value = parameters[1];

            if (key != null) {
                MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
                putIntoMap(key, value, myagg);
            }
        }

        @Override
        public Map<Object, Object> terminatePartial(
                @SuppressWarnings("deprecation") AggregationBuffer agg) throws HiveException {
            MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
            return myagg.container;
        }

        @Override
        public void merge(@SuppressWarnings("deprecation") AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
            Map<?, ?> partialResult = internalMergeOI.getMap(partial);
            for (Map.Entry<?, ?> entry : partialResult.entrySet()) {
                putIntoMap(entry.getKey(), entry.getValue(), myagg);
            }
        }

        @Override
        public Map<Object, Object> terminate(@SuppressWarnings("deprecation") AggregationBuffer agg)
                throws HiveException {
            MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
            return myagg.container;
        }

        protected void putIntoMap(Object key, Object value, MapAggregationBuffer myagg) {
            Object pKeyCopy = ObjectInspectorUtils.copyToStandardObject(key, this.inputKeyOI);
            Object pValueCopy = ObjectInspectorUtils.copyToStandardObject(value, this.inputValueOI);
            myagg.container.put(pKeyCopy, pValueCopy);
        }
    }

}
