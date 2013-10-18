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
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ArrayPrefixedHashValuesUDF extends UDF {

    public List<String> evaluate(List<String> values, String prefix) {
        if(values == null) {
            return null;
        }
        if(prefix == null) {
            prefix = "";
        }

        List<Integer> hashValues = ArrayHashValuesUDF.hashValues(values, null, MurmurHash3UDF.DEFAULT_NUM_FEATURES);
        final int len = hashValues.size();
        final String[] stringValues = new String[len];
        for(int i = 0; i < len; i++) {
            Integer v = hashValues.get(i);
            stringValues[i] = prefix + v.toString();
        }
        return Arrays.asList(stringValues);
    }
}
