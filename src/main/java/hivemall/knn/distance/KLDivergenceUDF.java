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
package hivemall.knn.distance;

import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;

@UDFType(deterministic = true, stateful = false)
public final class KLDivergenceUDF {

    public DoubleWritable evaluate(double mu1, double sigma1, double mu2, double sigma2) {
        double d = kld(mu1, sigma1, mu2, sigma2);
        return new DoubleWritable(d);
    }

    public FloatWritable evaluate(float mu1, float sigma1, float mu2, float sigma2) {
        float f = (float) kld(mu1, sigma1, mu2, sigma2);
        return new FloatWritable(f);
    }

    public static double kld(final double mu1, final double sigma1, final double mu2, final double sigma2) {
        return (Math.log(sigma2 / sigma1) + sigma2 / sigma1 + Math.pow(mu1 - mu2, 2) / sigma2 - 1.d) * 0.5d;
    }

}
