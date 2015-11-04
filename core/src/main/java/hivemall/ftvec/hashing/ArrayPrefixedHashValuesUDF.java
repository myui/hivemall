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
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ArrayPrefixedHashValuesUDF extends UDF {

    public List<Text> evaluate(List<String> values, String prefix) {
        return evaluate(values, prefix, false);
    }

    public List<Text> evaluate(List<String> values, String prefix, boolean useIndexAsPrefix) {
        if(values == null) {
            return null;
        }
        if(prefix == null) {
            prefix = "";
        }

        List<IntWritable> hashValues = ArrayHashValuesUDF.hashValues(values, null, MurmurHash3.DEFAULT_NUM_FEATURES, useIndexAsPrefix);
        final int len = hashValues.size();
        final Text[] stringValues = new Text[len];
        for(int i = 0; i < len; i++) {
            IntWritable v = hashValues.get(i);
            stringValues[i] = val(prefix + v.toString());
        }
        return Arrays.asList(stringValues);
    }
}
