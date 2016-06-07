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
package hivemall.knn.distance;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

@Description(name = "kld", value = "_FUNC_(double m1, double sigma1, double mu2, double sigma 2)"
        + " - Returns KL divergence between two distributions")
@UDFType(deterministic = true, stateful = false)
public final class KLDivergenceUDF extends UDF {

    public DoubleWritable evaluate(double mu1, double sigma1, double mu2, double sigma2) {
        double d = kld(mu1, sigma1, mu2, sigma2);
        return new DoubleWritable(d);
    }

    public FloatWritable evaluate(float mu1, float sigma1, float mu2, float sigma2) {
        float f = (float) kld(mu1, sigma1, mu2, sigma2);
        return new FloatWritable(f);
    }

    public static double kld(final double mu1, final double sigma1, final double mu2,
            final double sigma2) {
        return (Math.log(sigma2 / sigma1) + sigma2 / sigma1 + Math.pow(mu1 - mu2, 2) / sigma2 - 1.d) * 0.5d;
    }

}
