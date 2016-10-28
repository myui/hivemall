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

import static hivemall.utils.hadoop.WritableUtils.val;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

@Description(name = "map_get_sum", value = "_FUNC_(map<int,float> src, array<int> keys)"
        + " - Returns sum of values that are retrieved by keys")
@UDFType(deterministic = true, stateful = false)
public class MapGetSumUDF extends UDF {

    public DoubleWritable evaluate(Map<IntWritable, FloatWritable> map, List<IntWritable> keys) {
        double sum = 0d;
        for (IntWritable k : keys) {
            FloatWritable v = map.get(k);
            if (v != null) {
                sum += (double) v.get();
            }
        }
        return val(sum);
    }

}
