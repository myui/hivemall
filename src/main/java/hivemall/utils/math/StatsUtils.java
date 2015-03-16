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
package hivemall.utils.math;

public final class StatsUtils {

    private StatsUtils() {}

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
