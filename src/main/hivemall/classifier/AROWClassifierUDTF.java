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

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Adaptive Regularization of Weight Vectors (AROW) binary classifier.
 * <pre>
 * [1] K. Crammer, A. Kulesza, and M. Dredze, "Adaptive Regularization of Weight Vectors",
 *     In Proc. NIPS, 2009.
 * </pre>
 */
public class AROWClassifierUDTF extends BinaryOnlineClassifierUDTF {

    /** Regularization parameter r */
    protected float r;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("AROWClassifierUDTF takes 2 or 3 arguments: List<String|Int|BitInt> features, Int label [, constant String options]");
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
    protected void train(List<?> features, int label) {
        final int y = label > 0 ? 1 : -1;

        PredictionResult margin = calcScoreAndVariance(features);
        float m = margin.getScore() * y;

        if(m < 1.f) {
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
        final float old_w;
        final float old_cov;
        if(old == null) {
            old_w = 0.f;
            old_cov = 1.f;
        } else {
            old_w = old.getValue();
            old_cov = old.getCovariance();
        }

        float cv = old_cov * x;
        float new_w = old_w + (y * alpha * cv);
        float new_cov = old_cov - (beta * cv * cv);

        return new WeightValue(new_w, new_cov);
    }
}
