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
package hivemall.tools.macros;

import static hivemall.utils.WritableUtils.val;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.FloatWritable;

public class SigmodUDF extends UDF {

    public FloatWritable evaluate(float x) {
        return val(1.0f / (1.0f + (float) Math.exp(-x)));
    }

    public FloatWritable evaluate(double x) {
        float v = (float) (1.0 / (1.0 + Math.exp(-x)));
        return val(v);
    }

}
