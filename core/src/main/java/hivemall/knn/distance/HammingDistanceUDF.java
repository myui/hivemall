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
package hivemall.knn.distance;

import static hivemall.utils.hadoop.WritableUtils.val;

import java.math.BigInteger;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@Description(name = "hamming_distance",
        value = "_FUNC_(A, B [,int k]) - Returns Hamming distance between A and B")
@UDFType(deterministic = true, stateful = false)
public class HammingDistanceUDF extends UDF {

    public IntWritable evaluate(long a, long b) {
        return val(hammingDistance(a, b));
    }

    public IntWritable evaluate(String a, String b) {
        BigInteger ai = new BigInteger(a);
        BigInteger bi = new BigInteger(b);
        return val(hammingDistance(ai, bi));
    }

    public IntWritable evaluate(List<LongWritable> a, List<LongWritable> b) {
        int alen = a.size();
        int blen = b.size();

        final int min, max;
        final List<LongWritable> r;
        if (alen < blen) {
            min = alen;
            max = blen;
            r = b;
        } else {
            min = blen;
            max = alen;
            r = a;
        }

        int result = 0;
        for (int i = 0; i < min; i++) {
            result += hammingDistance(a.get(i).get(), b.get(i).get());
        }
        for (int j = min; j < max; j++) {
            result += hammingDistance(0L, r.get(j).get());
        }
        return val(result);
    }

    public static int hammingDistance(final long a, final long b) {
        return Long.bitCount(a ^ b);
    }

    public static int hammingDistance(final BigInteger a, final BigInteger b) {
        BigInteger xor = a.xor(b);
        return xor.bitCount();
    }

}
