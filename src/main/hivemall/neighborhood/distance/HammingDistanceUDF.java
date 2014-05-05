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
package hivemall.neighborhood.distance;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HammingDistanceUDF extends UDF {

    public int evaluate(Long a, Long b) {
        long xor = a.longValue() ^ b.longValue();
        return Long.bitCount(xor);
    }

    public int evaluate(List<Long> a, List<Long> b) {
        int alen = a.size();
        int blen = b.size();

        final int min, max;
        final List<Long> r;
        if(alen < blen) {
            min = alen;
            max = blen;
            r = b;
        } else {
            min = blen;
            max = alen;
            r = a;
        }

        int result = 0;
        for(int i = 0; i < min; i++) {
            long xor = a.get(i).longValue() ^ b.get(i).longValue();
            result += Long.bitCount(xor);
        }
        for(int j = min; j < max; j++) {
            long xor = 0L ^ r.get(j).longValue();
            result += Long.bitCount(xor);
        }
        return result;
    }

}
