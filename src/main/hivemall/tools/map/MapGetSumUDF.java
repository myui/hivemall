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
package hivemall.tools.map;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MapGetSumUDF extends UDF {

    public Double evaluate(Map<Integer, Float> map, List<Integer> keys) {
        double sum = 0d;
        for(Integer k : keys) {
            Float v = map.get(k);
            if(v != null) {
                sum += v.doubleValue();
            }
        }
        return sum;
    }

}
