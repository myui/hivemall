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

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

@Description(name = "mhash",
        value = "_FUNC_(string word) returns a murmurhash3 INT value starting from 1")
@UDFType(deterministic = true, stateful = false)
public final class MurmurHash3UDF extends UDF {

    @Nullable
    public IntWritable evaluate(@Nullable final String word) throws UDFArgumentException {
        return evaluate(word, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    @Nullable
    public IntWritable evaluate(@Nullable final String word, final int numFeatures)
            throws UDFArgumentException {
        if (word == null) {
            return null;
        }
        int h = mhash(word, numFeatures);
        return new IntWritable(h);
    }

    @Nullable
    public IntWritable evaluate(@Nullable final List<String> words) throws UDFArgumentException {
        return evaluate(words, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    @Nullable
    public IntWritable evaluate(@Nullable final List<String> words, final int numFeatures)
            throws UDFArgumentException {
        if (words == null) {
            return null;
        }
        final int size = words.size();
        if (size == 0) {
            return new IntWritable(1);
        }
        final StringBuilder b = new StringBuilder();
        b.append(words.get(0));
        for (int i = 1; i < size; i++) {
            b.append('\t');
            String v = words.get(i);
            b.append(v);
        }
        String s = b.toString();
        return evaluate(s, numFeatures);
    }

    public static int mhash(@Nonnull final String word) {
        return mhash(word, MurmurHash3.DEFAULT_NUM_FEATURES);
    }

    public static int mhash(@Nonnull final String word, final int numFeatures) {
        int r = MurmurHash3.murmurhash3_x86_32(word, 0, word.length(), 0x9747b28c) % numFeatures;
        if (r < 0) {
            r += numFeatures;
        }
        return r + 1;
    }

}
