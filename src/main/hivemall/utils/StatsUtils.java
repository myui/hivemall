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
package hivemall.utils;

public final class StatsUtils {

    /**
     * probit(p)=sqrt(2)erf^-1(2p-1)
     * <pre>
     * probit(1)=INF, probit(0)=-INF, probit(0.5)=0
     * </pre>
     * 
     * @param p must be in [0,1]
     * @link http://en.wikipedia.org/wiki/Probit
     */
    public static double probit(double p) {
        if(p < 0 || p > 1) {
            throw new IllegalArgumentException("p must be in [0,1]");
        }
        return Math.sqrt(2.d) * MathUtils.inverseErf(2.d * p - 1.d);
    }

    public static double probit(double p, double range) {
        if(range <= 0) {
            throw new IllegalArgumentException("range must be > 0: " + range);
        }
        if(p == 0) {
            return -range;
        }
        if(p == 1) {
            return range;
        }
        double v = probit(p);
        if(v < 0) {
            return Math.max(v, -range);
        } else {
            return Math.min(v, range);
        }
    }
}
