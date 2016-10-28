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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

@Description(name = "sha1", value = "_FUNC_(string word [, int numFeatures]) returns a SHA-1 value")
@UDFType(deterministic = true, stateful = false)
public final class Sha1UDF extends UDF {

    public static final int DEFAULT_NUM_FEATURES = 16777216;

    private final MessageDigest sha256;

    public Sha1UDF() {
        super();
        try {
            sha256 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public IntWritable evaluate(String word) {
        return evaluate(word, DEFAULT_NUM_FEATURES);
    }

    public IntWritable evaluate(String word, boolean rawValue) {
        if (rawValue) {
            int sha1 = sha1(word);
            return new IntWritable(sha1);
        } else {
            return evaluate(word, DEFAULT_NUM_FEATURES);
        }
    }

    public IntWritable evaluate(String word, int numFeatures) {
        int r = sha1(word) % numFeatures;
        if (r < 0) {
            r += numFeatures;
        }
        return new IntWritable(r + 1);
    }

    public IntWritable evaluate(List<String> words) {
        return evaluate(words, DEFAULT_NUM_FEATURES);
    }

    public IntWritable evaluate(List<String> words, int numFeatures) {
        final int wlength = words.size();
        if (wlength == 0) {
            return new IntWritable(1);
        }
        final StringBuilder b = new StringBuilder();
        b.append(words.get(0));
        for (int i = 1; i < wlength; i++) {
            b.append('\t');
            b.append(words.get(i));
        }
        String s = b.toString();
        return evaluate(s, numFeatures);
    }

    private int sha1(final String word) {
        final MessageDigest md;
        try {
            md = (MessageDigest) sha256.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
        byte[] in = getBytes(word);
        byte[] di = md.digest(in);
        return getInt(di, 0);
    }

    private static byte[] getBytes(final String s) {
        final int len = s.length();
        final byte[] b = new byte[len * 2];
        for (int i = 0; i < len; i++) {
            putChar(b, i << 1, s.charAt(i));
        }
        return b;
    }

    private static void putChar(final byte[] b, final int off, final char val) {
        b[off + 1] = (byte) (val >>> 0);
        b[off] = (byte) (val >>> 8);
    }

    private static int getInt(final byte[] b, final int off) {
        return ((b[3] & 0xFF) << 24) | ((b[2] & 0xFF) << 16) | ((b[1] & 0xFF) << 8) | (b[0] & 0xFF);
    }
}
