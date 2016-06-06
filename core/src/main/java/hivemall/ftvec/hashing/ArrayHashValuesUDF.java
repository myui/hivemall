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
package hivemall.ftvec.hashing;

import static hivemall.utils.hadoop.WritableUtils.val;
import hivemall.utils.hashing.MurmurHash3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

@Description(
        name = "array_hash_values",
        value = "_FUNC_(array<string> values, [string prefix [, int numFeatures], boolean useIndexAsPrefix])"
                + " returns hash values in array<int>")
@UDFType(deterministic = true, stateful = false)
public final class ArrayHashValuesUDF extends UDF {

    public List<IntWritable> evaluate(List<String> values) {
        return evaluate(values, null, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    public List<IntWritable> evaluate(List<String> values, String prefix) {
        return evaluate(values, prefix, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    public List<IntWritable> evaluate(List<String> values, String prefix, boolean useIndexAsPrefix) {
        return evaluate(values, prefix, MurmurHash3.DEFAULT_NUM_FEATURES, useIndexAsPrefix);
    }

    public List<IntWritable> evaluate(List<String> values, String prefix, int numFeatures) {
        return evaluate(values, prefix, numFeatures, false);
    }

    public List<IntWritable> evaluate(List<String> values, String prefix, int numFeatures,
            boolean useIndexAsPrefix) {
        return hashValues(values, prefix, numFeatures, useIndexAsPrefix);
    }

    static List<IntWritable> hashValues(List<String> values, String prefix, int numFeatures,
            boolean useIndexAsPrefix) {
        if (values == null) {
            return null;
        }
        if (values.isEmpty()) {
            return Collections.emptyList();
        }
        final int size = values.size();
        final IntWritable[] ary = new IntWritable[size];
        for (int i = 0; i < size; i++) {
            String v = values.get(i);
            if (v == null) {
                ary[i] = null;
            } else {
                if (useIndexAsPrefix) {
                    v = i + ':' + v;
                }
                String data = (prefix == null) ? v : (prefix + v);
                int h = MurmurHash3.murmurhash3(data, numFeatures);
                ary[i] = val(h + 1);
            }
        }
        return Arrays.asList(ary);
    }

}
