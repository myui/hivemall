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
package hivemall.ftvec;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

@Description(name = "sort_by_feature", value = "_FUNC_(map in map<int,float>) - Returns a sorted map")
@UDFType(deterministic = true, stateful = false)
public final class SortByFeatureUDF extends UDF {

    public Map<IntWritable, FloatWritable> evaluate(Map<IntWritable, FloatWritable> arg) {
        Map<IntWritable, FloatWritable> ret = new TreeMap<IntWritable, FloatWritable>();
        ret.putAll(arg);
        return ret;
    }

}
