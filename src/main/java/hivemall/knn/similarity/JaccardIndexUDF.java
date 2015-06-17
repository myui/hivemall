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
package hivemall.knn.similarity;

import static hivemall.utils.hadoop.WritableUtils.val;
import hivemall.knn.distance.HammingDistanceUDF;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;

@Description(name = "jaccard", value = "_FUNC_(A, B [,int k]) - Returns Jaccard similarity coefficient of A and B")
@UDFType(deterministic = true, stateful = false)
public final class JaccardIndexUDF extends UDF {

    private final Set<Object> union = new HashSet<Object>();
    private final Set<Object> intersect = new HashSet<Object>();

    public FloatWritable evaluate(long a, long b) {
        return evaluate(a, b, 128);
    }

    public FloatWritable evaluate(long a, long b, int k) {
        int countMatches = k - HammingDistanceUDF.hammingDistance(a, b);
        float jaccard = countMatches / (float) k;
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

    public FloatWritable evaluate(final List<String> a, final List<String> b) {
        if(a == null || b == null) {
            return new FloatWritable(0.f);
        }
        if(a.size() == 0 || b.size() == 0) {
            return new FloatWritable(0.f);
        }

        union.addAll(a);
        union.addAll(b);
        float unionSize = union.size();
        union.clear();

        intersect.addAll(a);
        intersect.retainAll(b);
        float intersectSize = intersect.size();
        intersect.clear();

        return new FloatWritable(intersectSize / unionSize);
    }

}
