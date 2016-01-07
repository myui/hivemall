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
package hivemall.regression;

import hivemall.common.LossFunctions;
import hivemall.common.OnlineVariance;
import hivemall.io.FeatureValue;
import hivemall.io.IWeightValue;
import hivemall.io.PredictionResult;
import hivemall.io.WeightValue.WeightValueWithCovar;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class AROWRegressionUDTF extends RegressionBaseUDTF {

    /** Regularization parameter r */
    protected float r;

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
    protected boolean useCovariance() {
        return true;
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("r", "regularization", true, "Regularization parameter for some r > 0 [default 0.1]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);

        float r = 0.1f;
        if(cl != null) {
            String r_str = cl.getOptionValue("r");
            if(r_str != null) {
                r = Float.parseFloat(r_str);
                if(!(r > 0)) {
                    throw new UDFArgumentException("Regularization parameter must be greater than 0: "
                            + r_str);
                }
            }
        }

        this.r = r;
        return cl;
    }

    @Override
    protected void train(@Nonnull final FeatureValue[] features, float target) {
        PredictionResult margin = calcScoreAndVariance(features);
        float predicted = margin.getScore();

        float loss = loss(target, predicted);

        float var = margin.getVariance();
        float beta = 1.f / (var + r);

        update(features, loss, beta);
    }

    /**
     * @return target - predicted
     */
    protected float loss(float target, float predicted) {
        return target - predicted; // y - m^Tx
    }

    @Override
    protected void update(@Nonnull final FeatureValue[] features, final float coeff, final float beta) {
        for(FeatureValue f : features) {
            if(f == null) {
                continue;
            }
            Object k = f.getFeature();
            float v = f.getValueAsFloat();

            IWeightValue old_w = model.get(k);
            IWeightValue new_w = getNewWeight(old_w, v, coeff, beta);
            model.set(k, new_w);
        }
    }

    private static IWeightValue getNewWeight(final IWeightValue old, final float x, final float coeff, final float beta) {
        final float old_w;
        final float old_cov;
        if(old == null) {
            old_w = 0.f;
            old_cov = 1.f;
        } else {
            old_w = old.get();
            old_cov = old.getCovariance();
        }

        float cov_x = old_cov * x;
        float new_w = old_w + coeff * cov_x * beta;
        float new_cov = old_cov - (beta * cov_x * cov_x);

        return new WeightValueWithCovar(new_w, new_cov);
    }

    public static class AROWe extends AROWRegressionUDTF {

        /** Sensitivity to prediction mistakes */
        protected float epsilon;

        @Override
        protected Options getOptions() {
            Options opts = super.getOptions();
            opts.addOption("e", "epsilon", true, "Sensitivity to prediction mistakes [default 0.1]");
            return opts;
        }

        @Override
        protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
            CommandLine cl = super.processOptions(argOIs);

            float epsilon = 0.1f;
            if(cl != null) {
                String opt_epsilon = cl.getOptionValue("epsilon");
                if(opt_epsilon != null) {
                    epsilon = Float.parseFloat(opt_epsilon);
                }
            }

            this.epsilon = epsilon;
            return cl;
        }

        @Override
        protected void train(@Nonnull final FeatureValue[] features, float target) {
            preTrain(target);

            PredictionResult margin = calcScoreAndVariance(features);
            float predicted = margin.getScore();

            float loss = loss(target, predicted);
            if(loss > 0.f) {
                float coeff = (target - predicted) > 0.f ? loss : -loss;
                float var = margin.getVariance();
                float beta = 1.f / (var + r);
                update(features, coeff, beta);
            }
        }

        protected void preTrain(float target) {}

        /** 
         * |w^t - y| - epsilon 
         */
        protected float loss(float target, float predicted) {
            return LossFunctions.epsilonInsensitiveLoss(predicted, target, epsilon);
        }
    }

    public static class AROWe2 extends AROWe {

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
