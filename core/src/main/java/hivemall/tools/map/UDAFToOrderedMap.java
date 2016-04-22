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

import hivemall.utils.hadoop.HiveUtils;

import java.util.Collections;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Convert two aggregated columns into a sorted key-value map.
 */
@Description(name = "to_ordered_map",
        value = "_FUNC_(key, value [, const boolean reverseOrder=false]) "
                + "- Convert two aggregated columns into an ordered key-value map")
public class UDAFToOrderedMap extends UDAFToMap {

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
            throws SemanticException {
        @SuppressWarnings("deprecation")
        TypeInfo[] typeInfo = info.getParameters();
        if (typeInfo.length != 2 && typeInfo.length != 3) {
            throw new UDFArgumentTypeException(typeInfo.length - 1,
                "Expecting two or three arguments: " + typeInfo.length);
        }
        if (typeInfo[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                "Only primitive type arguments are accepted for the key but "
                        + typeInfo[0].getTypeName() + " was passed as parameter 1.");
        }
        boolean reverseOrder = false;
        if (typeInfo.length == 3) {
            if (HiveUtils.isBooleanTypeInfo(typeInfo[2]) == false) {
                throw new UDFArgumentTypeException(2, "The three argument must be boolean type: "
                        + typeInfo[2].getTypeName());
            }
            ObjectInspector[] argOIs = info.getParameterObjectInspectors();
            reverseOrder = HiveUtils.getConstBoolean(argOIs[2]);
        }

        if (reverseOrder) {
            return new ReverseOrdereMapEvaluator();
        } else {
            return new NaturalOrdereMapEvaluator();
        }
    }

    public static class NaturalOrdereMapEvaluator extends UDAFToMapEvaluator {

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MapAggregationBuffer) agg).container = new TreeMap<Object, Object>();
        }

    }

    public static class ReverseOrdereMapEvaluator extends UDAFToMapEvaluator {

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MapAggregationBuffer) agg).container = new TreeMap<Object, Object>(
                Collections.reverseOrder());
        }

    }

}
