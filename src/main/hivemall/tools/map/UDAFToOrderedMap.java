/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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