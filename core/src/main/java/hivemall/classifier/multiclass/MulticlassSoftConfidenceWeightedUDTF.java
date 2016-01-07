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
package hivemall.classifier.multiclass;

import hivemall.io.FeatureValue;
import hivemall.io.IWeightValue;
import hivemall.io.Margin;
import hivemall.io.PredictionModel;
import hivemall.io.WeightValue.WeightValueWithCovar;
import hivemall.utils.math.StatsUtils;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Soft Confidence-Weighted binary classifier.
 * <pre>
 * [1] Steven C. H. Hoi, Jialei Wang, Peilin Zhao: Exact Soft Confidence-Weighted Learning. ICML 2012
 * </pre>
 * 
 * @link http://icml.cc/2012/papers/86.pdf
 */
public abstract class MulticlassSoftConfidenceWeightedUDTF extends MulticlassOnlineClassifierUDTF {

    /** Confidence parameter phi */
    protected float phi;

    /** Aggressiveness parameter */
    protected float c;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("MulticlassSoftConfidenceWeightedUDTF takes 2 or 3 arguments: List<String|Int|BitInt> features, {Int|String} label [, constant String options]");
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
        opts.addOption("phi", "confidence", true, "Confidence parameter [default 1.0]");
        opts.addOption("eta", "hyper_c", true, "Confidence hyperparameter eta in range (0.5, 1] [default 0.85]");
        opts.addOption("c", "aggressiveness", true, "Aggressiveness parameter C [default 1.0]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);

        float phi = 1.f;
        float c = 1.f;
        if(cl != null) {
            String phi_str = cl.getOptionValue("phi");
            if(phi_str == null) {
                String eta_str = cl.getOptionValue("eta");
                if(eta_str != null) {
                    double eta = Double.parseDouble(eta_str);
                    if(eta <= 0.5 || eta > 1) {
                        throw new UDFArgumentException("Confidence hyperparameter eta must be in range (0.5, 1]: "
                                + eta_str);
                    }
                    phi = (float) StatsUtils.probit(eta, 5d);
                }
            } else {
                phi = Float.parseFloat(phi_str);
            }

            String c_str = cl.getOptionValue("c");
            if(c_str != null) {
                c = Float.parseFloat(c_str);
                if(!(c > 0.f)) {
                    throw new UDFArgumentException("Aggressiveness parameter C must be C > 0: " + c);
                }
            }
        }

        this.phi = phi;
        this.c = c;

        return cl;
    }

    @Override
    protected void train(@Nonnull final FeatureValue[] features, @Nonnull Object actual_label) {
        Margin margin = getMarginAndVariance(features, actual_label, true);
        float loss = loss(margin);

        if(loss > 0.f) {
            float alpha = getAlpha(margin);
            if(alpha == 0.f) {
                return;
            }
            float beta = getBeta(margin, alpha);
            if(beta == 0.f) {
                return;
            }

            Object missed_label = margin.getMaxIncorrectLabel();
            update(features, actual_label, missed_label, alpha, beta);
        }
    }

    protected float loss(Margin margin) {
        float var = margin.getVariance();
        float m = margin.get();
        assert (var != 0);
        float loss = phi * (float) Math.sqrt(var) - m;
        return Math.max(loss, 0.f);
    }

    protected abstract float getAlpha(Margin margin);

    protected abstract float getBeta(Margin margin, float alpha);

    public static class SCW1 extends MulticlassSoftConfidenceWeightedUDTF {

        private float squared_phi, psi, zeta;

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            StructObjectInspector oi = super.initialize(argOIs);

            float phiphi = phi * phi;
            this.squared_phi = phiphi;
            this.psi = 1.f + phiphi / 2.f;
            this.zeta = 1.f + phiphi;

            return oi;
        }

        @Override
        protected float getAlpha(Margin margin) {
            float m = margin.get();
            float var = margin.getVariance();

            float alpha_numer = -m
                    * psi
                    + (float) Math.sqrt((m * m * squared_phi * squared_phi / 4.f)
                            + (var * squared_phi * zeta));
            float alpha_denom = var * zeta;
            if(alpha_denom == 0.f) {
                return 0.f;
            }
            float alpha = alpha_numer / alpha_denom;

            if(alpha <= 0.f) {
                return 0.f;
            }

            return Math.max(c, alpha);
        }

        @Override
        protected float getBeta(Margin margin, float alpha) {
            if(alpha == 0.f) {
                return 0.f;
            }
            float var = margin.getVariance();

            float beta_numer = alpha * phi;
            float var_alpha_phi = var * beta_numer;
            float u = -var_alpha_phi + (float) Math.sqrt(var_alpha_phi * var_alpha_phi + 4.f * var);
            float beta_den = u / 2.f + var_alpha_phi;
            if(beta_den == 0.f) {
                return 0.f;
            }

            float beta = beta_numer / beta_den;
            return beta;
        }

    }

    public static class SCW2 extends SCW1 {

        @Override
        protected float getAlpha(Margin margin) {
            float m = margin.get();
            float var = margin.getVariance();

            float squared_phi = phi * phi;

            float n = var + c / 2.f;
            float v_phi_phi = var * squared_phi;
            float v_phi_phi_m = v_phi_phi * m;
            float term = v_phi_phi_m * m * var + 4.f * n * var * (n + v_phi_phi);
            float gamma = phi * (float) Math.sqrt(term);

            float alpha_numer = -(2.f * m * n + v_phi_phi_m) + gamma;
            if(alpha_numer <= 0.f) {
                return 0.f;
            }
            float alpha_denom = 2.f * (n * n + n * v_phi_phi);
            if(alpha_denom == 0.f) {
                return 0.f;
            }
            float alpha = alpha_numer / alpha_denom;

            return Math.max(0.f, alpha);
        }

    }

    protected void update(@Nonnull final FeatureValue[] features, final Object actual_label, final Object missed_label, final float alpha, final float beta) {
        assert (actual_label != null);
        if(actual_label.equals(missed_label)) {
            throw new IllegalArgumentException("Actual label equals to missed label: "
                    + actual_label);
        }

        PredictionModel model2add = label2model.get(actual_label);
        if(model2add == null) {
            model2add = createModel();
            label2model.put(actual_label, model2add);
        }
        PredictionModel model2sub = null;
        if(missed_label != null) {
            model2sub = label2model.get(missed_label);
            if(model2sub == null) {
                model2sub = createModel();
                label2model.put(missed_label, model2sub);
            }
        }

        for(FeatureValue f : features) {// w[f] += y * x[f]
            if(f == null) {
                continue;
            }
            final Object k = f.getFeature();
            final float v = f.getValueAsFloat();

            IWeightValue old_correctclass_w = model2add.get(k);
            IWeightValue new_correctclass_w = getNewWeight(old_correctclass_w, v, alpha, beta, true);
            model2add.set(k, new_correctclass_w);

            if(model2sub != null) {
                IWeightValue old_wrongclass_w = model2sub.get(k);
                IWeightValue new_wrongclass_w = getNewWeight(old_wrongclass_w, v, alpha, beta, false);
                model2sub.set(k, new_wrongclass_w);
            }
        }
    }

    private static IWeightValue getNewWeight(final IWeightValue old, final float v, final float alpha, final float beta, final boolean positive) {
        final float old_v;
        final float old_cov;
        if(old == null) {
            old_v = 0.f;
            old_cov = 1.f;
        } else {
            old_v = old.get();
            old_cov = old.getCovariance();
        }

        float cv = old_cov * v;
        float new_w = positive ? old_v + (alpha * cv) : old_v - (alpha * cv);
        float new_cov = old_cov - (beta * cv * cv);

        return new WeightValueWithCovar(new_w, new_cov);
    }
}
