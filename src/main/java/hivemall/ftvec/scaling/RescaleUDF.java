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
 * @see <a href="http://en.wikipedia.org/wiki/Feature_scaling">Feature_scaling</a>
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

    private static float min_max_normalization(final float value, final float min, final float max) {
        return (value - min) / (max - min);
    }

}
