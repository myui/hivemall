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
import hivemall.common.PredictionModel;
import hivemall.common.WeightValue;
import hivemall.common.WeightValue.WeightValueWithCovar;

import java.util.List;

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

        final ObjectInspector featureInspector = featureListOI.getListElementObjectInspector();
        for(Object f : features) {// w[f] += y * x[f]
            if(f == null) {
                continue;
            }
            final Object k;
            final float v;
            if(parseFeature) {
                FeatureValue fv = FeatureValue.parse(f);
                k = fv.getFeature();
                v = fv.getValue();
            } else {
                k = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                v = 1.f;
            }
            WeightValue old_correctclass_w = model2add.get(k);
            WeightValue new_correctclass_w = getNewWeight(old_correctclass_w, v, alpha, beta, true);
            model2add.set(k, new_correctclass_w);

            if(model2sub != null) {
                WeightValue old_wrongclass_w = model2sub.get(k);
                WeightValue new_wrongclass_w = getNewWeight(old_wrongclass_w, v, alpha, beta, false);
                model2sub.set(k, new_wrongclass_w);
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
            old_v = old.get();
            old_cov = old.getCovariance();
        }

        float cv = old_cov * v;
        float new_w = positive ? old_v + (alpha * cv) : old_v - (alpha * cv);
        float new_cov = old_cov - (beta * cv * cv);

        return new WeightValueWithCovar(new_w, new_cov);
    }

    public static class AROWh extends MulticlassAROWClassifierUDTF {

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
            if(cl != null) {
                String c_str = cl.getOptionValue("c");
                if(c_str != null) {
                    c = Float.parseFloat(c_str);
                    if(!(c > 0.f)) {
                        throw new UDFArgumentException("Aggressiveness parameter C must be C > 0: "
                                + c);
                    }
                }
            }

            this.c = c;
            return cl;
        }

        @Override
        protected void train(List<?> features, Object actual_label) {
            Margin margin = getMarginAndVariance(features, actual_label);

            float loss = loss(margin);
            if(loss > 0.f) {
                float var = margin.getVariance();
                float beta = 1.f / (var + r);
                float alpha = loss * beta;

                Object missed_label = margin.getMaxIncorrectLabel();
                update(features, actual_label, missed_label, alpha, beta);
            }
        }

        /** 
         * @return C - m
         */
        protected float loss(Margin margin) {
            return c - margin.get();
        }
    }
}
