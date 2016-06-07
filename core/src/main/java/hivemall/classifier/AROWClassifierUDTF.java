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
package hivemall.classifier;

import hivemall.common.LossFunctions;
import hivemall.model.FeatureValue;
import hivemall.model.IWeightValue;
import hivemall.model.PredictionResult;
import hivemall.model.WeightValue.WeightValueWithCovar;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Adaptive Regularization of Weight Vectors (AROW) binary classifier.
 * 
 * <pre>
 * [1] K. Crammer, A. Kulesza, and M. Dredze, "Adaptive Regularization of Weight Vectors",
 *     In Proc. NIPS, 2009.
 * </pre>
 */
@Description(
        name = "train_arow",
        value = "_FUNC_(list<string|int|bigint> features, int label [, const string options])"
                + " - Returns a relation consists of <string|int|bigint feature, float weight, float covar>",
        extended = "Build a prediction model by Adaptive Regularization of Weight Vectors (AROW) binary classifier")
public class AROWClassifierUDTF extends BinaryOnlineClassifierUDTF {

    /** Regularization parameter r */
    protected float r;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if (numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException(
                "_FUNC_ takes 2 or 3 arguments: List<String|Int|BitInt> features, Int label [, constant String options]");
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
        opts.addOption("r", "regularization", true,
            "Regularization parameter for some r > 0 [default 0.1]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);

        float r = 0.1f;
        if (cl != null) {
            String r_str = cl.getOptionValue("r");
            if (r_str != null) {
                r = Float.parseFloat(r_str);
                if (!(r > 0)) {
                    throw new UDFArgumentException(
                        "Regularization parameter must be greater than 0: " + r_str);
                }
            }
        }

        this.r = r;
        return cl;
    }

    @Override
    protected void train(@Nonnull final FeatureValue[] features, int label) {
        final float y = label > 0 ? 1.f : -1.f;

        PredictionResult margin = calcScoreAndVariance(features);
        float m = margin.getScore() * y;

        if (m < 1.f) {
            float var = margin.getVariance();
            float beta = 1.f / (var + r);
            float alpha = (1.f - m) * beta;
            update(features, y, alpha, beta);
        }
    }

    protected float loss(PredictionResult margin, float y) {
        float m = margin.getScore() * y;
        return m < 0.f ? 1.f : 0.f; // suffer loss = 1 if sign(t) != y
    }

    protected void update(@Nonnull final FeatureValue[] features, final float y, final float alpha,
            final float beta) {
        for (FeatureValue f : features) {
            if (f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValueAsFloat();

            IWeightValue old_w = model.get(k);
            IWeightValue new_w = getNewWeight(old_w, v, y, alpha, beta);
            model.set(k, new_w);
        }
    }

    private static IWeightValue getNewWeight(final IWeightValue old, final float x, final float y,
            final float alpha, final float beta) {
        final float old_w;
        final float old_cov;
        if (old == null) {
            old_w = 0.f;
            old_cov = 1.f;
        } else {
            old_w = old.get();
            old_cov = old.getCovariance();
        }

        float cv = old_cov * x;
        float new_w = old_w + (y * alpha * cv);
        float new_cov = old_cov - (beta * cv * cv);

        return new WeightValueWithCovar(new_w, new_cov);
    }

    @Description(
            name = "train_arow_h",
            value = "_FUNC_(list<string|int|bigint> features, int label [, const string options])"
                    + " - Returns a relation consists of <string|int|bigint feature, float weight, float covar>",
            extended = "Build a prediction model by AROW binary classifier using hinge loss")
    public static class AROWh extends AROWClassifierUDTF {

        /** Aggressiveness parameter */
        protected float c;

        @Override
        protected Options getOptions() {
            Options opts = super.getOptions();
            opts.addOption("c", "aggressiveness", true, "Aggressiveness parameter C [default 1.0]");
            return opts;
        }

        @Override
        protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
            final CommandLine cl = super.processOptions(argOIs);

            float c = 1.f;
            if (cl != null) {
                String c_str = cl.getOptionValue("c");
                if (c_str != null) {
                    c = Float.parseFloat(c_str);
                    if (!(c > 0.f)) {
                        throw new UDFArgumentException("Aggressiveness parameter C must be C > 0: "
                                + c);
                    }
                }
            }

            this.c = c;
            return cl;
        }

        @Override
        protected void train(@Nonnull final FeatureValue[] features, int label) {
            final float y = label > 0 ? 1.f : -1.f;

            PredictionResult margin = calcScoreAndVariance(features);
            float p = margin.getScore();
            float loss = loss(p, y); // C - m (m = y * p)

            if (loss > 0.f) {// m < 1.0 || 1.0 - m > 0 
                float var = margin.getVariance();
                float beta = 1.f / (var + r);
                float alpha = loss * beta; // (1.f - m) * beta
                update(features, y, alpha, beta);
            }
        }

        /**
         * @return C - y * p
         */
        protected float loss(final float p, final float y) {
            return LossFunctions.hingeLoss(p, y, c);
        }
    }
}
