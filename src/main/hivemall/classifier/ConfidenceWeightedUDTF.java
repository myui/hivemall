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
 * Confidence-weighted linear classification.
 * <pre>
 * [1] Mark Dredze, Koby Crammer and Fernando Pereira. "Confidence-weighted linear classification",
 *     In Proc. ICML, pp.264-271, 2008.
 * </pre>
 * 
 * @link http://dl.acm.org/citation.cfm?id=1390190
 */
public class ConfidenceWeightedUDTF extends BinaryOnlineClassifierUDTF {

    /** confidence parameter phi */
    protected float phi;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("ConfidenceWeightedUDTF takes 2 or 3 arguments: List<String|Int|BitInt> features, Int label [, constant String options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("phi", "confidence", true, "Confidence parameter [default 0.5]");
        opts.addOption("eta", "hyper_c", true, "Confidence hyperparameter eta in range (0.5, 1] [default 0.7]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);

        float phi = 0.5f;
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
        }

        this.phi = phi;
        return cl;
    }

    @Override
    protected void train(List<?> features, int label) {
        final int y = label > 0 ? 1 : -1;

        PredictionResult margin = calcScoreAndVariance(features);
        float gamma = getGamma(margin, y);

        if(gamma > 0.f) {// alpha = max(0, gamma)
            float coeff = gamma * y;
            update(features, coeff, gamma);
        }
    }

    protected final float getGamma(PredictionResult margin, int y) {
        float score = margin.getScore() * y;
        float var = margin.getVariance();

        float b = 1.f + 2.f * phi * score;
        float gamma_numer = -b + (float) Math.sqrt(b * b - 8.f * phi * (score - phi * var));
        float gamma_denom = 4.f * phi * var;
        if(gamma_denom == 0.f) {// avoid divide-by-zero
            return 0.f;
        }
        return gamma_numer / gamma_denom;
    }

    protected void update(final List<?> features, final float coeff, final float alpha) {
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
            WeightValue new_w = getNewWeight(old_w, v, coeff, alpha, phi);
            weights.put(k, new_w);
        }

        if(biasKey != null) {
            WeightValue old_bias = weights.get(biasKey);
            WeightValue new_bias = getNewWeight(old_bias, bias, coeff, alpha, phi);
            weights.put(biasKey, new_bias);
        }
    }

    private static WeightValue getNewWeight(final WeightValue old, final float x, final float coeff, final float alpha, final float phi) {
        final float old_w, old_cov;
        if(old == null) {
            old_w = 0.f;
            old_cov = 1.f;
        } else {
            old_w = old.get();
            old_cov = old.getCovariance();
        }

        float new_w = old_w + (coeff * old_cov * x);
        float new_cov = 1.f / (1.f / old_cov + (2.f * alpha * phi * x * x));
        return new WeightValue(new_w, new_cov);
    }
}
