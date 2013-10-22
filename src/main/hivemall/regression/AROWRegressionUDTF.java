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

import hivemall.common.FeatureValue;
import hivemall.common.LossFunctions;
import hivemall.common.PredictionResult;
import hivemall.common.WeightValue;

import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class AROWRegressionUDTF extends OnlineRegressionUDTF {

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
    protected void train(Collection<?> features, float target) {
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
    protected void update(final Collection<?> features, final float coeff, final float beta) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();

        for(Object f : features) {
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseX) {
                FeatureValue fv = FeatureValue.parse(f, feature_hashing);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            WeightValue old_w = weights.get(k);
            WeightValue new_w = getNewWeight(old_w, v, coeff, beta);
            weights.put(k, new_w);
        }

        if(biasKey != null) {
            WeightValue old_bias = weights.get(biasKey);
            WeightValue new_bias = getNewWeight(old_bias, bias, coeff, beta);
            weights.put(biasKey, new_bias);
        }
    }

    private static WeightValue getNewWeight(final WeightValue old, final float x, final float coeff, final float beta) {
        final float old_w;
        final float old_cov;
        if(old == null) {
            old_w = 0.f;
            old_cov = 1.f;
        } else {
            old_w = old.getValue();
            old_cov = old.getCovariance();
        }

        float cov_x = old_cov * x;
        float new_w = old_w + coeff * cov_x * beta;
        float new_cov = old_cov - (beta * cov_x * cov_x);

        return new WeightValue(new_w, new_cov);
    }

    public static class AROWh extends AROWRegressionUDTF {

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
        protected void train(Collection<?> features, float target) {
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

        /** 
         * |w^t - y| - epsilon 
         */
        protected float loss(float target, float predicted) {
            return LossFunctions.epsilonInsensitiveLoss(predicted, target, epsilon);
        }
    }

}
