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
        float gamma = getGamma(margin);

        if(gamma > 0.f) {// alpha = max(0, gamma)
            float coeff = gamma * y;
            update(features, coeff, gamma);
        }
    }

    protected final float getGamma(PredictionResult margin) {
        float score = margin.getScore();
        float var = margin.getVariance();

        float b = 1.f + 2.f * phi * score;
        float gamma = (-b + (float) Math.sqrt(b * b - 8.f * phi * (score - phi * var))) / 4.f * phi
                * var;
        return gamma;
    }

    protected void update(final List<?> features, final float coeff, final float alpha) {
        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();

        for(Object f : features) {
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
            float new_w = (old_w == null) ? coeff * v : old_w.getValue() + (coeff * v);
            float old_cov = (old_w == null) ? 1.f : old_w.getCovariance();
            float new_cov = 1.f / (1.f / old_cov + (2.f * alpha * phi * v * v));
            weights.put(k, new WeightValue(new_w, new_cov));
        }

        if(biasKey != null) {
            WeightValue old_bias = weights.get(biasKey);
            float new_bias = (old_bias == null) ? coeff * bias : old_bias.getValue()
                    + (coeff * bias);
            float old_cov = (old_bias == null) ? 1.f : old_bias.getCovariance();
            float new_cov = 1.f / (1.f / old_cov + (2.f * alpha * phi * bias * bias));
            weights.put(biasKey, new WeightValue(new_bias, new_cov));
        }
    }
}
