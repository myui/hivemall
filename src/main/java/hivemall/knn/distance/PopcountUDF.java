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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

@UDFType(deterministic = true, stateful = false)
public final class PopcountUDF extends UDF {

    public IntWritable evaluate(long a) {
        return val(Long.bitCount(a));
    }

    public IntWritable evaluate(String a) {
        BigInteger ai = new BigInteger(a);
        return val(ai.bitCount());
    }

    public IntWritable evaluate(List<Long> a) {
        int result = 0;
        for(int i = 0; i < a.size(); i++) {
            long x = a.get(i).longValue();
            result += Long.bitCount(x);
        }
        return val(result);
    }

    /**
     * Count bits that both bits are 1.
     */
    public IntWritable evaluate(long a, long b) {
        long innerProduct = a & b;
        return val(Long.bitCount(innerProduct));
    }

    /**
     * Count bits that both bits are 1.
     */
    public IntWritable evaluate(String a, String b) {
        BigInteger ai = new BigInteger(a);
        BigInteger bi = new BigInteger(b);
        BigInteger innerProduct = ai.and(bi);
        return val(innerProduct.bitCount());
    }

    /**
     * Count bits that both bits are 1.
     */
    public IntWritable evaluate(List<Long> a, List<Long> b) {
        final int min = Math.min(a.size(), b.size());
        int result = 0;
        for(int i = 0; i < min; i++) {
            long innerProduct = a.get(i).longValue() & b.get(i).longValue();
            result += Long.bitCount(innerProduct);
        }
        return val(result);
    }

}
