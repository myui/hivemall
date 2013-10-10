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
package hivemall.tools.array;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;

public class SubarrayEndWithUDF extends UDF {

    public List<Integer> evaluate(List<Integer> original, int key) {
        if(original == null) {
            return null;
        }
        int toIndex = original.lastIndexOf(Integer.valueOf(key));
        if(toIndex == -1) {
            return null;
        }
        return original.subList(0, toIndex + 1);
    }

    public List<String> evaluate(List<String> original, String key) {
        if(original == null) {
            return null;
        }
        int toIndex = original.lastIndexOf(key);
        if(toIndex == -1) {
            return null;
        }
        return original.subList(0, toIndex + 1);
    }

}
