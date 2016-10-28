/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.common;

import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Primitives;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public abstract class EtaEstimator {

    protected final float eta0;

    public EtaEstimator(float eta0) {
        this.eta0 = eta0;
    }

    public float eta0() {
        return eta0;
    }

    public abstract float eta(long t);

    public void update(@Nonnegative float multipler) {}

    public static final class FixedEtaEstimator extends EtaEstimator {

        public FixedEtaEstimator(float eta) {
            super(eta);
        }

        @Override
        public float eta(long t) {
            return eta0;
        }

    }

    public static final class SimpleEtaEstimator extends EtaEstimator {

        private final float finalEta;
        private final double total_steps;

        public SimpleEtaEstimator(float eta0, long total_steps) {
            super(eta0);
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

        private final double power_t;

        public InvscalingEtaEstimator(float eta0, double power_t) {
            super(eta0);
            this.power_t = power_t;
        }

        @Override
        public float eta(final long t) {
            return (float) (eta0 / Math.pow(t, power_t));
        }

    }

    /**
     * bold driver: Gemulla et al., Large-scale matrix factorization with distributed stochastic
     * gradient descent, KDD 2011.
     */
    public static final class AdjustingEtaEstimator extends EtaEstimator {

        private float eta;

        public AdjustingEtaEstimator(float eta) {
            super(eta);
            this.eta = eta;
        }

        @Override
        public float eta(long t) {
            return eta;
        }

        @Override
        public void update(@Nonnegative float multipler) {
            float newEta = eta * multipler;
            if (!NumberUtils.isFinite(newEta)) {
                // avoid NaN or INFINITY
                return;
            }
            this.eta = Math.min(eta0, newEta); // never be larger than eta0
        }

    }

    @Nonnull
    public static EtaEstimator get(@Nullable CommandLine cl) throws UDFArgumentException {
        return get(cl, 0.1f);
    }

    @Nonnull
    public static EtaEstimator get(@Nullable CommandLine cl, float defaultEta0)
            throws UDFArgumentException {
        if (cl == null) {
            return new InvscalingEtaEstimator(defaultEta0, 0.1d);
        }

        if (cl.hasOption("boldDriver")) {
            float eta = Primitives.parseFloat(cl.getOptionValue("eta"), 0.3f);
            return new AdjustingEtaEstimator(eta);
        }

        String etaValue = cl.getOptionValue("eta");
        if (etaValue != null) {
            float eta = Float.parseFloat(etaValue);
            return new FixedEtaEstimator(eta);
        }

        float eta0 = Primitives.parseFloat(cl.getOptionValue("eta0"), defaultEta0);
        if (cl.hasOption("t")) {
            long t = Long.parseLong(cl.getOptionValue("t"));
            return new SimpleEtaEstimator(eta0, t);
        }

        double power_t = Primitives.parseDouble(cl.getOptionValue("power_t"), 0.1d);
        return new InvscalingEtaEstimator(eta0, power_t);
    }

}
