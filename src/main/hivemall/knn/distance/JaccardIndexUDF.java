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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.FloatWritable;

public final class JaccardIndexUDF extends UDF {

    public FloatWritable evaluate(long a, long b) {
        return evaluate(a, b, 128);
    }

    public FloatWritable evaluate(long a, long b, int k) {
        int countMatches = k - HammingDistanceUDF.hammingDistance(a, b);
        float jaccard = countMatches / k;
        return val(2.f * (jaccard - 0.5f));
    }

    public FloatWritable evaluate(String a, String b) {
        return evaluate(a, b, 128);
    }

    public FloatWritable evaluate(String a, String b, int k) {
        BigInteger ai = new BigInteger(a);
        BigInteger bi = new BigInteger(b);
        int countMatches = k - HammingDistanceUDF.hammingDistance(ai, bi);
        float jaccard = countMatches / (float) k;
        return val(2.f * (jaccard - 0.5f));
    }
}
