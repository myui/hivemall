/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.knn.distance;

import static hivemall.utils.hadoop.WritableUtils.val;

import java.math.BigInteger;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

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
