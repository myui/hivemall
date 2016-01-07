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
package hivemall.regression;

import hivemall.common.LossFunctions;
import hivemall.io.FeatureValue;
import hivemall.io.IWeightValue;
import hivemall.io.WeightValue.WeightValueParamsF1;
import hivemall.utils.lang.Primitives;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * ADAGRAD algorithm with element-wise adaptive learning rates. 
 */
public final class AdaGradUDTF extends RegressionBaseUDTF {

    private float eta;
    private float eps;
    private float scaling;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("AdagradUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, float target [, constant string options]");
        }

        StructObjectInspector oi = super.initialize(argOIs);
        model.configureParams(true, false, false);
        return oi;
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("eta", "eta0", true, "The initial learning rate [default 1.0]");
        opts.addOption("eps", true, "A constant used in the denominator of AdaGrad [default 1.0]");
        opts.addOption("scale", true, "Internal scaling/descaling factor for cumulative weights [100]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        if(cl == null) {
            this.eta = 1.f;
            this.eps = 1.f;
            this.scaling = 100f;
        } else {
            this.eta = Primitives.parseFloat(cl.getOptionValue("eta"), 1.f);
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
    protected void update(@Nonnull final FeatureValue[] features, float target, float predicted) {
        float gradient = LossFunctions.logisticLoss(target, predicted);
        onlineUpdate(features, gradient);
    }

    @Override
    protected void onlineUpdate(@Nonnull final FeatureValue[] features, float gradient) {
        final float g_g = gradient * (gradient / scaling);

        for(FeatureValue f : features) {// w[i] += y * x[i]
            if(f == null) {
                continue;
            }
            Object x = f.getFeature();
            float xi = f.getValueAsFloat();

            IWeightValue old_w = model.get(x);
            IWeightValue new_w = getNewWeight(old_w, xi, gradient, g_g);
            model.set(x, new_w);
        }
    }

    @Nonnull
    protected IWeightValue getNewWeight(@Nullable final IWeightValue old, final float xi, final float gradient, final float g_g) {
        float old_w = 0.f;
        float scaled_sum_sqgrad = 0.f;

        if(old != null) {
            old_w = old.get();
            scaled_sum_sqgrad = old.getSumOfSquaredGradients();
        }
        scaled_sum_sqgrad += g_g;

        float coeff = eta(scaled_sum_sqgrad) * gradient;
        float new_w = old_w + (coeff * xi);
        return new WeightValueParamsF1(new_w, scaled_sum_sqgrad);
    }

    protected float eta(final double scaledSumOfSquaredGradients) {
        double sumOfSquaredGradients = scaledSumOfSquaredGradients * scaling;
        //return eta / (float) Math.sqrt(sumOfSquaredGradients);
        return eta / (float) Math.sqrt(eps + sumOfSquaredGradients); // always less than eta0
    }

}
