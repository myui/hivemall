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
 * Multi-class Adaptive Regularization of Weight Vectors (AROW) classifier.
 * <pre>
 * [1] K. Crammer, A. Kulesza, and M. Dredze, "Adaptive Regularization of Weight Vectors",
 *     In Proc. NIPS, 2009.
 * </pre>
 */
public class MulticlassAROWClassifierUDTF extends MulticlassOnlineClassifierUDTF {

    /** Regularization parameter r */
    protected float r;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("MulticlassAROWClassifierUDTF takes 2 or 3 arguments: List<String|Int|BitInt> features, {Int|String} label [, constant String options]");
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
    protected void train(List<?> features, Object actual_label) {
        Margin margin = getMarginAndVariance(features, actual_label);
        float m = margin.get();

        if(m >= 1.f) {
            return;
        }

        float var = margin.getVariance();
        float beta = 1.f / (var + r);
        float alpha = (1.f - m) * beta;

        Object missed_label = margin.getMaxIncorrectLabel();
        update(features, actual_label, missed_label, alpha, beta);
    }

    protected void update(final List<?> features, final Object actual_label, final Object missed_label, final float alpha, final float beta) {
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
            WeightValue old_correctclass_w = weightsToAdd.get(k);
            WeightValue new_correctclass_w = getNewWeight(old_correctclass_w, v, alpha, beta, true);
            weightsToAdd.put(k, new_correctclass_w);

            if(weightsToSub != null) {
                WeightValue old_wrongclass_w = weightsToSub.get(k);
                WeightValue new_wrongclass_w = getNewWeight(old_wrongclass_w, v, alpha, beta, false);
                weightsToSub.put(k, new_wrongclass_w);
            }
        }

        if(biasKey != null) {
            WeightValue old_correctclass_bias = weightsToAdd.get(biasKey);
            WeightValue new_correctclass_bias = getNewWeight(old_correctclass_bias, bias, alpha, beta, true);
            weightsToAdd.put(biasKey, new_correctclass_bias);

            if(weightsToSub != null) {
                WeightValue old_wrongclass_bias = weightsToSub.get(biasKey);
                WeightValue new_wrongclass_bias = getNewWeight(old_wrongclass_bias, bias, alpha, beta, false);
                weightsToSub.put(biasKey, new_wrongclass_bias);
            }
        }
    }

    private static WeightValue getNewWeight(final WeightValue old, final float v, final float alpha, final float beta, final boolean positive) {
        final float old_v;
        final float old_cov;
        if(old == null) {
            old_v = 0.f;
            old_cov = 1.f;
        } else {
            old_v = old.getValue();
            old_cov = old.getCovariance();
        }

        float cv = old_cov * v;
        float new_w = positive ? old_v + (alpha * cv) : old_v - (alpha * cv);
        float new_cov = old_cov - (beta * cv * cv);

        return new WeightValue(new_w, new_cov);
    }

}
