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

import hivemall.common.LossFunctions;
import hivemall.io.FeatureValue;
import hivemall.io.IWeightValue;
import hivemall.io.WeightValue.WeightValueWithGt;
import hivemall.utils.lang.Primitives;

import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * ADAGRAD algorithm with element-wise adaptive learning rates. 
 */
public final class AdaGradUDTF extends OnlineRegressionUDTF {

    private float eta0;
    private float eps;
    private float scaling;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("AdagradUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, float target [, constant string options]");
        }

        StructObjectInspector oi = super.initialize(argOIs);
        model.configurParams(true);
        return oi;
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("eta0", true, "The initial learning rate [default 0.1]");
        opts.addOption("eps", true, "A constant used in the denominator of AdaGrad [default 1.0]");
        opts.addOption("scale", true, "Internal scaling/descaling factor for cumulative weights [100]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        if(cl == null) {
            this.eta0 = 0.1f;
            this.eps = 1.f;
            this.scaling = 100f;
        } else {
            this.eta0 = Primitives.parseFloat(cl.getOptionValue("eta0"), 0.1f);
            this.eps = Primitives.parseFloat(cl.getOptionValue("eps"), 1.f);
            this.scaling = Primitives.parseFloat(cl.getOptionValue("scale"), 100f);
        }
        return cl;
    }

    @Override
    protected final void checkTargetValue(final float target) throws UDFArgumentException {
        if(target < 0.f || target > 1.f) {
            throw new UDFArgumentException("target must be in range 0 to 1: " + target);
        }
    }

    @Override
    protected void update(Collection<?> features, float target, float predicted) {
        float gradient = LossFunctions.logisticLoss(target, predicted);
        update(features, gradient);
    }

    protected void update(Collection<?> features, float gradient) {
        final ObjectInspector featureInspector = this.featureInputOI;
        final float g_g = gradient * (gradient / scaling);

        for(Object f : features) {// w[i] += y * x[i]
            if(f == null) {
                continue;
            }
            final Object x;
            final float xi;
            if(parseFeature) {
                FeatureValue fv = FeatureValue.parse(f);
                x = fv.getFeature();
                xi = fv.getValue();
            } else {
                x = ObjectInspectorUtils.copyToStandardObject(f, featureInspector);
                xi = 1.f;
            }

            float old_w = 0.f;
            float scaled_sum_sqgrad = 0.f;
            IWeightValue old = model.get(x);
            if(old != null) {
                old_w = old.get();
                scaled_sum_sqgrad = old.getSumOfSquaredGradients();
            }
            scaled_sum_sqgrad += g_g;

            float coeff = eta(scaled_sum_sqgrad) * gradient;
            float new_w = old_w + (coeff * xi);
            model.set(x, new WeightValueWithGt(new_w, scaled_sum_sqgrad));
        }
    }

    protected float eta(final double scaledSumOfSquaredGradients) {
        double sumOfSquaredGradients = scaledSumOfSquaredGradients * scaling;
        //return eta0 / (float) Math.sqrt(sumOfSquaredGradients);
        return eta0 / (float) Math.sqrt(eps + sumOfSquaredGradients); // always less than eta0
    }

}
