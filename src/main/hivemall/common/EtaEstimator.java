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
package hivemall.common;

public interface EtaEstimator {

    public float eta(int t);

    public static class SimpleEtaEstimator implements EtaEstimator {

        private final float eta0;
        private final float total_steps;

        public SimpleEtaEstimator(float eta0, int total_steps) {
            this.eta0 = eta0;
            this.total_steps = total_steps;
        }

        @Override
        public float eta(int t) {
            if(t > total_steps) {
                return eta0 / 2.f;
            }
            return eta0 / (1.f + (t / total_steps));
        }

    }

    public static class InvscalingEtaEstimator implements EtaEstimator {

        private final float eta0;
        private final double power_t;

        public InvscalingEtaEstimator(float eta0, float power_t) {
            this.eta0 = eta0;
            this.power_t = power_t;
        }

        @Override
        public float eta(int t) {
            return eta0 / (float) Math.pow(t, power_t);
        }

    }

}
