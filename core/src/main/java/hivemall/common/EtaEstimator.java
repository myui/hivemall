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
package hivemall.common;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public abstract class EtaEstimator {

    public abstract float eta(long t);

    public static final class FixedEtaEstimator extends EtaEstimator {

        private final float eta;

        public FixedEtaEstimator(float eta) {
            this.eta = eta;
        }

        @Override
        public float eta(long t) {
            return eta;
        }

    }

    public static final class SimpleEtaEstimator extends EtaEstimator {

        private final double eta0;
        private final float finalEta;
        private final double total_steps;

        public SimpleEtaEstimator(double eta0, long total_steps) {
            this.eta0 = eta0;
            this.finalEta = (float) (eta0 / 2.d);
            this.total_steps = total_steps;
        }

        @Override
        public float eta(final long t) {
            if (t > total_steps) {
                return finalEta;
            }
            return (float) (eta0 / (1.d + (t / total_steps)));
        }

    }

    public static final class InvscalingEtaEstimator extends EtaEstimator {

        private final double eta0;
        private final double power_t;

        public InvscalingEtaEstimator(double eta0, double power_t) {
            this.eta0 = eta0;
            this.power_t = power_t;
        }

        @Override
        public float eta(final long t) {
            return (float) (eta0 / Math.pow(t, power_t));
        }

    }

    /**
     * bold driver: Gemulla et al., Large-scale matrix factorization with distributed stochastic gradient descent, KDD 2011.
     */
    public static final class AdjustingEtaEstimator extends EtaEstimator {

        private final float eta0;
        private float eta;

        public AdjustingEtaEstimator(float eta) {
            this.eta0 = eta;
            this.eta = eta;
        }

        public void update(@Nonnegative float multipler) {
            this.eta = Math.max(eta0, eta * multipler);
        }

        @Override
        public float eta(long t) {
            return eta;
        }

    }

    @Nonnull
    public static EtaEstimator get(@Nullable CommandLine cl) throws UDFArgumentException {
        if (cl == null) {
            return new InvscalingEtaEstimator(0.1f, 0.1f);
        }

        String etaValue = cl.getOptionValue("eta");
        if (etaValue != null) {
            float eta = Float.parseFloat(etaValue);
            return new FixedEtaEstimator(eta);
        }

        double eta0 = Double.parseDouble(cl.getOptionValue("eta0", "0.1"));
        if (cl.hasOption("t")) {
            long t = Long.parseLong(cl.getOptionValue("t"));
            return new SimpleEtaEstimator(eta0, t);
        }

        double power_t = Double.parseDouble(cl.getOptionValue("power_t", "0.1"));
        return new InvscalingEtaEstimator(eta0, power_t);
    }

}
