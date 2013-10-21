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
package hivemall.regression;

import hivemall.common.LossFunctions;
import hivemall.common.OnlineVariance;
import hivemall.common.PredictionResult;

import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class PassiveAggressiveRegressionUDTF extends OnlineRegressionUDTF {

    /** Aggressiveness parameter */
    protected float c;

    /** Sensitivity to prediction mistakes */
    protected float epsilon;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes arguments: List<Int|BigInt|Text> features, float target [, constant string options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("c", "aggressiveness", true, "Aggressiveness paramete [default Float.MAX_VALUE]");
        opts.addOption("e", "epsilon", true, "Sensitivity to prediction mistakes [default 0.1]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);

        float c = aggressiveness();
        float epsilon = 0.1f;

        if(cl != null) {
            String opt_c = cl.getOptionValue("c");
            if(opt_c != null) {
                c = Float.parseFloat(opt_c);
                if(!(c > 0.f)) {
                    throw new UDFArgumentException("Aggressiveness parameter C must be C > 0: " + c);
                }
            }

            String opt_epsilon = cl.getOptionValue("epsilon");
            if(opt_epsilon != null) {
                epsilon = Float.parseFloat(opt_epsilon);
            }
        }

        this.c = c;
        this.epsilon = epsilon;
        return cl;
    }

    protected float aggressiveness() {
        return Float.MAX_VALUE;
    }

    @Override
    protected void train(Collection<?> features, float target) {
        preTrain(target);

        PredictionResult margin = calcScoreAndNorm(features);
        float predicted = margin.getScore();
        float loss = loss(target, predicted);

        if(loss > 0.f) {
            int sign = (target - predicted) > 0.f ? 1 : -1; // sign(y - (W^t)x)
            float eta = eta(loss, margin); // min(C, loss / |x|^2)
            float coeff = sign * eta;
            if(!Float.isInfinite(coeff)) {
                update(features, coeff);
            }
        }
    }

    protected void preTrain(float target) {}

    /** 
     * |w^t - y| - epsilon 
     */
    protected float loss(float target, float predicted) {
        return LossFunctions.epsilonInsensitiveLoss(predicted, target, epsilon);
    }

    /**
     * min(C, loss / |x|^2)
     */
    protected float eta(float loss, PredictionResult margin) {
        float squared_norm = margin.getSquaredNorm();
        float eta = loss / squared_norm;
        return Math.min(c, eta);
    }

    public static class PA1a extends PassiveAggressiveRegressionUDTF {

        private OnlineVariance targetStdDev;

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            this.targetStdDev = new OnlineVariance();
            return super.initialize(argOIs);
        }

        @Override
        protected void preTrain(float target) {
            targetStdDev.handle(target);
        }

        @Override
        protected float loss(float target, float predicted) {
            float stddev = (float) targetStdDev.stddev();
            float e = epsilon * stddev;
            return LossFunctions.epsilonInsensitiveLoss(predicted, target, e);
        }

    }

    public static class PA2 extends PassiveAggressiveRegressionUDTF {

        @Override
        protected float aggressiveness() {
            return 1.f;
        }

        @Override
        protected float eta(float loss, PredictionResult margin) {
            float squared_norm = margin.getSquaredNorm();
            float eta = loss / (squared_norm + (0.5f / c));
            return eta;
        }

    }

    public static class PA2a extends PA2 {

        private OnlineVariance targetStdDev;

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            this.targetStdDev = new OnlineVariance();
            return super.initialize(argOIs);
        }

        @Override
        protected void preTrain(float target) {
            targetStdDev.handle(target);
        }

        @Override
        protected float loss(float target, float predicted) {
            float stddev = (float) targetStdDev.stddev();
            float e = epsilon * stddev;
            return LossFunctions.epsilonInsensitiveLoss(predicted, target, e);
        }

    }

}
