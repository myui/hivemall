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
package hivemall.ftvec.hashing;

import hivemall.utils.hashing.MurmurHash3;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@Description(name = "prefixed_hash_values",
        value = "_FUNC_(array<string> values, string prefix [, boolean useIndexAsPrefix])"
                + " returns array<string> that each element has the specified prefix")
@UDFType(deterministic = true, stateful = false)
public final class ArrayPrefixedHashValuesUDF extends UDF {

    public List<Text> evaluate(List<String> values, String prefix) {
        return evaluate(values, prefix, false);
    }

    public List<Text> evaluate(List<String> values, String prefix, boolean useIndexAsPrefix) {
        if (values == null) {
            return null;
        }
        if (prefix == null) {
            prefix = "";
        }

        List<IntWritable> hashValues = ArrayHashValuesUDF.hashValues(values, null,
            MurmurHash3.DEFAULT_NUM_FEATURES, useIndexAsPrefix);
        final int len = hashValues.size();
        final Text[] stringValues = new Text[len];
        for (int i = 0; i < len; i++) {
            IntWritable v = hashValues.get(i);
            stringValues[i] = new Text(prefix + v.toString());
        }
        return Arrays.asList(stringValues);
    }
}
