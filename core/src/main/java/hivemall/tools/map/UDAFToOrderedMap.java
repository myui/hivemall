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
package hivemall.tools.map;

import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Convert two aggregated columns into a sorted key-value map.
 * 
 * The key must be a primitive type (int, boolean, float, string, ...) and the
 * value may be a primitive or a complex type (structs, maps, arrays).
 * 
 * https://cwiki.apache.org/Hive/genericudafcasestudy.html
 */
@Description(name = "to_ordered_map", value = "_FUNC_(key, value) - Convert two aggregated columns into an ordered key-value map")
public class UDAFToOrderedMap extends UDAFToMap {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        super.getEvaluator(parameters);
        return new UDAFToOrderedMapEvaluator();
    }

    public static class UDAFToOrderedMapEvaluator extends UDAFToMapEvaluator {

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkMapAggregationBuffer) agg).container = new TreeMap<Object, Object>();
        }

    }

}
