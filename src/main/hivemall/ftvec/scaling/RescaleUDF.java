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
package hivemall.ftvec.scaling;

import static hivemall.utils.hadoop.WritableUtils.val;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

/**
 * Min-Max normalization 
 * 
 * @see http://en.wikipedia.org/wiki/Feature_scaling
 */
@Description(name = "rescale", value = "_FUNC_(value, min, max) - Returns rescaled value by min-max normalization")
@UDFType(deterministic = true, stateful = false)
public final class RescaleUDF extends UDF {

    public FloatWritable evaluate(final float value, final float min, final float max) {
        return val(min_max_normalization(value, min, max));
    }

    public FloatWritable evaluate(final float value, final double min, final double max) {
        return val(min_max_normalization(value, (float) min, (float) max));
    }

    public static float min_max_normalization(final float value, final float min, final float max) {
        return (value - min) / (max - min);
    }

    public Text evaluate(final String s, final double min, final double max) {
        return evaluate(s, (float) min, (float) max);
    }

    public Text evaluate(final String s, final float min, final float max) {
        String[] fv = s.split(":");
        if(fv.length != 2) {
            throw new IllegalArgumentException("Invalid feature value representation: " + s);
        }
        float v = Float.parseFloat(fv[1]);
        float scaled_v = min_max_normalization(v, min, max);
        String ret = fv[0] + ':' + Float.toString(scaled_v);
        return val(ret);
    }

}
