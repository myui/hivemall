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
package hivemall.classifier.multiclass;

import hivemall.common.FeatureValue;
import hivemall.common.Margin;
import hivemall.common.WeightValue;
import hivemall.utils.StatsUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * A multi-class confidence-weighted linear classification.
 * <pre>
 * [1] Mark Dredze, Koby Crammer and Fernando Pereira. "Confidence-weighted linear classification",
 *     In Proc. ICML, pp.264-271, 2008.
 * [2] Koby Crammer, Mark Dredze and Alex Kulesza. "Multi-class confidence weighted algorithms",
 *     In Proc. EMNLP, Vol. 2, pp.496-504, 2008.
 * </pre>
 * 
 * @link http://dl.acm.org/citation.cfm?id=1390190
 * @link http://dl.acm.org/citation.cfm?id=1699577
 */
public class MulticlassConfidenceWeightedUDTF extends MulticlassOnlineClassifierUDTF {

    /** confidence parameter phi */
    protected float phi;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("MultiClassConfidenceWeightedUDTF takes 2 or 3 arguments: List<String|Int|BitInt> features, {Int|String} label [, constant String options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("phi", "confidence", true, "Confidence parameter [default 0.5]");
        opts.addOption("eta", "hyper_c", true, "Confidence hyperparameter eta in range (0, 1) [default 0.7]");
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
                        throw new UDFArgumentException("Confidence hyperparameter eta must be in range (0.5,1]: "
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
    protected void train(List<?> features, Object actual_label) {
        Margin margin = getMarginAndVariance(features, actual_label);
        float gamma = getGamma(margin);

        if(gamma > 0.f) {// alpha = max(0, gamma)                   
            Object missed_label = margin.getMaxIncorrectLabel();
            update(features, gamma, actual_label, missed_label, gamma, phi);
        }
    }

    protected final float getGamma(Margin margin) {
        float score = margin.get();
        float var = margin.getVariance();

        float b = 1.f + 2.f * phi * score;
        float gamma = (-b + (float) Math.sqrt(b * b - 8.f * phi * (score - phi * var))) / 4.f * phi
                * var;
        return gamma;
    }

    protected void update(List<?> features, float coeff, Object actual_label, Object missed_label, final float alpha, final float phi) {
        assert (actual_label != null);
        if(actual_label.equals(missed_label)) {
            throw new IllegalArgumentException("Actual label equals to missed label: "
                    + actual_label);
        }

        Map<Object, WeightValue> weightsToAdd = label2FeatureWeight.get(actual_label);
        if(weightsToAdd == null) {
            weightsToAdd = new HashMap<Object, WeightValue>(8192);
            label2FeatureWeight.put(actual_label, weightsToAdd);
        }
        Map<Object, WeightValue> weightsToSub = null;
        if(missed_label != null) {
            weightsToSub = label2FeatureWeight.get(missed_label);
            if(weightsToSub == null) {
                weightsToSub = new HashMap<Object, WeightValue>(8192);
                label2FeatureWeight.put(missed_label, weightsToSub);
            }
        }

        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();

        for(Object f : features) {// w[f] += y * x[f]
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
            WeightValue old_correctclass_w = weightsToAdd.get(k);
            float add_w = (old_correctclass_w == null) ? coeff * v : old_correctclass_w.getValue()
                    + (coeff * v);
            float new_correctcov = covariance(old_correctclass_w, v, alpha, phi);
            weightsToAdd.put(k, new WeightValue(add_w, new_correctcov));

            if(weightsToSub != null) {
                WeightValue old_wrongclass_w = weightsToSub.get(k);
                float sub_w = (old_wrongclass_w == null) ? -(coeff * v)
                        : old_wrongclass_w.getValue() - (coeff * v);
                float new_wrongcov = covariance(old_wrongclass_w, v, alpha, phi);
                weightsToSub.put(k, new WeightValue(sub_w, new_wrongcov));
            }
        }

        if(biasKey != null) {
            WeightValue old_correctclass_bias = weightsToAdd.get(biasKey);
            float add_bias = (old_correctclass_bias == null) ? coeff * bias
                    : old_correctclass_bias.getValue() + (coeff * bias);
            float new_correctbias_cov = covariance(old_correctclass_bias, bias, alpha, phi);
            weightsToAdd.put(biasKey, new WeightValue(add_bias, new_correctbias_cov));

            if(weightsToSub != null) {
                WeightValue old_wrongclass_bias = weightsToSub.get(biasKey);
                float sub_bias = (old_wrongclass_bias == null) ? -(coeff * bias)
                        : old_wrongclass_bias.getValue() - (coeff * bias);
                float new_wrongbias_cov = covariance(old_wrongclass_bias, bias, alpha, phi);
                weightsToSub.put(biasKey, new WeightValue(sub_bias, new_wrongbias_cov));
            }
        }
    }

    private static float covariance(final WeightValue old_w, final float v, final float alpha, final float phi) {
        float old_cov = (old_w == null) ? 1.f : old_w.getCovariance();
        return 1.f / (1.f / old_cov + (2.f * alpha * phi * v * v));
    }

}
