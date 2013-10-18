/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
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
package hivemall.ftvec.hashing;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ArrayHashValuesUDF extends UDF {

    public List<Integer> evaluate(List<String> values) {
        return evaluate(values, null, MurmurHash3UDF.DEFAULT_NUM_FEATURES);
    }

    public List<Integer> evaluate(List<String> values, String prefix) {
        return evaluate(values, prefix, MurmurHash3UDF.DEFAULT_NUM_FEATURES);
    }

    public List<Integer> evaluate(List<String> values, String prefix, int numFeatures) {
        return hashValues(values, prefix, numFeatures);
    }

    static List<Integer> hashValues(List<String> values, String prefix, int numFeatures) {
        if(values == null) {
            return null;
        }
        if(values.isEmpty()) {
            return Collections.emptyList();
        }
        final int size = values.size();
        final Integer[] ary = new Integer[size];
        for(int i = 0; i < size; i++) {
            String v = values.get(i);
            if(v == null) {
                ary[i] = null;
            } else {
                String data = (prefix == null) ? (i + ':' + v) : (prefix + i + ':' + v);
                ary[i] = MurmurHash3UDF.murmurhash3(data, numFeatures);
            }
        }
        return Arrays.asList(ary);
    }

}