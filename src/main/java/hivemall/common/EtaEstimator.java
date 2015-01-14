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
            if(t > total_steps) {
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

    @Nonnull
    public static EtaEstimator get(@Nullable CommandLine cl) throws UDFArgumentException {
        if(cl == null) {
            return new InvscalingEtaEstimator(0.2f, 0.1f);
        }

        String etaValue = cl.getOptionValue("eta");
        if(etaValue != null) {
            float eta = Float.parseFloat(etaValue);
            return new FixedEtaEstimator(eta);
        }

        double eta0 = Double.parseDouble(cl.getOptionValue("eta0", "0.2"));
        if(cl.hasOption("t")) {
            long t = Long.parseLong(cl.getOptionValue("t"));
            return new SimpleEtaEstimator(eta0, t);
        }

        double power_t = Double.parseDouble(cl.getOptionValue("power_t", "0.1"));
        return new InvscalingEtaEstimator(eta0, power_t);
    }

}
