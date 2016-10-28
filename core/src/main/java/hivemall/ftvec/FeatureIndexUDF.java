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
package hivemall.ftvec;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

@Description(
        name = "feature_index",
        value = "_FUNC_(feature_vector in array<string>) - Returns feature indicies in array<index>")
@UDFType(deterministic = true, stateful = false)
public final class FeatureIndexUDF extends UDF {

    @Nullable
    public IntWritable evaluate(@Nullable String feature) throws HiveException {
        if (feature == null) {
            return null;
        }
        int idx = featureIndex(feature);
        return new IntWritable(idx);
    }

    @Nullable
    public List<IntWritable> evaluate(@Nullable List<String> featureVector) throws HiveException {
        if (featureVector == null) {
            return null;
        }
        final int size = featureVector.size();
        final List<IntWritable> list = new ArrayList<IntWritable>(size);
        for (String f : featureVector) {
            if (f != null) {
                int fi = featureIndex(f);
                list.add(new IntWritable(fi));
            }
        }
        return list;
    }

    private static int featureIndex(@Nonnull final String feature) throws HiveException {
        final String f = extractFeature(feature);
        try {
            return Integer.parseInt(f);
        } catch (NumberFormatException e) {
            throw new HiveException(e);
        }
    }

    @Nonnull
    private static String extractFeature(@Nonnull final String feature) {
        final int pos = feature.indexOf(":");
        if (pos > 0) {
            return feature.substring(0, pos);
        } else {
            return feature;
        }
    }
}
