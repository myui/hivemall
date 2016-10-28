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
package hivemall.ftvec.conv;

import hivemall.utils.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

@Description(name = "to_sparse_features", value = "_FUNC_(array<float> feature_vector)"
        + " - Returns a sparse feature in array<string>")
@UDFType(deterministic = true, stateful = false)
public final class ToSparseFeaturesUDF extends UDF {

    @Nullable
    public List<String> evaluate(@Nullable final List<FloatWritable> features) {
        return evaluate(features, null);
    }

    @Nullable
    public List<String> evaluate(@Nullable final List<FloatWritable> features,
            @Nullable String biasName) {
        if (features == null) {
            return null;
        }
        final int size = features.size();
        if (size == 0) {
            return Collections.emptyList();
        }

        final StringBuilder buf = new StringBuilder(64);
        final ArrayList<String> list = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            final FloatWritable o = features.get(i);
            if (o != null) {
                final String s;
                final float v = o.get();
                if (biasName != null) {
                    s = buf.append(biasName).append(':').append(v).toString();
                } else {
                    s = buf.append(i).append(':').append(v).toString();
                }
                list.add(s);
                StringUtils.clear(buf);
            }
        }
        return list;
    }

}
