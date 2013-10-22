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
package hivemall.classifier;

import hivemall.common.FeatureValue;
import hivemall.common.PredictionResult;
import hivemall.common.WeightValue;
import hivemall.utils.StatsUtils;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Soft Confidence-Weighted binary classifier.
 * <pre>
 * [1] Steven C. H. Hoi, Jialei Wang, Peilin Zhao: Exact Soft Confidence-Weighted Learning. ICML 2012
 * </pre>
 * 
 * @link http://icml.cc/2012/papers/86.pdf
 */
public abstract class SoftConfideceWeightedUDTF extends BinaryOnlineClassifierUDTF {

    /** Confidence parameter phi */
    protected float phi;

    /** Aggressiveness parameter */
    protected float c;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("SoftConfideceWeightedUDTF takes 2 or 3 arguments: List<String|Int|BitInt> features, Int label [, constant String options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("phi", "confidence", true, "Confidence parameter [default 0.5]");
        opts.addOption("eta", "hyper_c", true, "Confidence hyperparameter eta in range (0.5, 1] [default 0.7]");
        opts.addOption("c", "aggressiveness", true, "Aggressiveness parameter C [default 1.0]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);

        float phi = 0.5f;
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
    protected void train(List<?> features, int label) {
        final int y = label > 0 ? 1 : -1;

        PredictionResult margin = calcScoreAndVariance(features);
        float loss = loss(margin, y);

        if(loss > 0.f) {
            float alpha = getAlpha(margin);
            if(alpha == 0.f) {
                return;
            }
            float beta = getBeta(margin, alpha);
            if(beta == 0.f) {
                return;
            }
            update(features, y, alpha, beta);
        }

    }

    protected float loss(PredictionResult margin, float y) {
        float var = margin.getVariance();
        float mean = margin.getScore();
        float loss = phi * (float) Math.sqrt(var) - (y * mean);
        return Math.max(loss, 0.f);
    }

    protected abstract float getAlpha(PredictionResult margin);

    protected abstract float getBeta(PredictionResult margin, float alpha);

    public static class SCW1 extends SoftConfideceWeightedUDTF {

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
        protected float getAlpha(PredictionResult margin) {
            float m = margin.getScore();
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
        protected float getBeta(PredictionResult margin, float alpha) {
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
        protected float getAlpha(PredictionResult margin) {
            float m = margin.getScore();
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

    protected void update(final List<?> features, final int y, final float alpha, final float beta) {
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
            WeightValue new_w = getNewWeight(old_w, v, y, alpha, beta);
            weights.put(k, new_w);
        }

        if(biasKey != null) {
            WeightValue old_bias = weights.get(biasKey);
            WeightValue new_bias = getNewWeight(old_bias, bias, y, alpha, beta);
            weights.put(biasKey, new_bias);
        }
    }

    private static WeightValue getNewWeight(final WeightValue old, final float x, final float y, final float alpha, final float beta) {
        final float old_v;
        final float old_cov;
        if(old == null) {
            old_v = 0.f;
            old_cov = 1.f;
        } else {
            old_v = old.getValue();
            old_cov = old.getCovariance();
        }

        float cv = old_cov * x;
        float new_w = old_v + (y * alpha * cv);
        float new_cov = old_cov - (beta * cv * cv);

        return new WeightValue(new_w, new_cov);
    }
}
