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
import hivemall.io.WeightValue.WeightValueParamsF2;
import hivemall.utils.lang.Primitives;

import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * ADADELTA: AN ADAPTIVE LEARNING RATE METHOD.
 */
public final class AdaDeltaUDTF extends OnlineRegressionUDTF {

    private float decay;
    private float eps;
    private float scaling;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("AdaDeltaUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, float target [, constant string options]");
        }

        StructObjectInspector oi = super.initialize(argOIs);
        model.configureParams(true, true, false);
        return oi;
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("rho", "decay", true, "Decay rate [default 0.95]");
        opts.addOption("eps", true, "A constant used in the denominator of AdaGrad [default 1e-6]");
        opts.addOption("scale", true, "Internal scaling/descaling factor for cumulative weights [100]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        if(cl == null) {
            this.decay = 0.95f;
            this.eps = 1e-6f;
            this.scaling = 100f;
        } else {
            this.decay = Primitives.parseFloat(cl.getOptionValue("decay"), 0.95f);
            this.eps = Primitives.parseFloat(cl.getOptionValue("eps"), 1E-6f);
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

    @Override
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

            IWeightValue old_w = model.get(x);
            IWeightValue new_w = getNewWeight(old_w, xi, gradient, g_g);
            model.set(x, new_w);
        }
    }

    @Nonnull
    protected IWeightValue getNewWeight(@Nullable final IWeightValue old, final float xi, final float gradient, final float g_g) {
        float old_w = 0.f;
        float old_scaled_sum_sqgrad = 0.f;
        float old_sum_squared_delta_x = 0.f;
        if(old != null) {
            old_w = old.get();
            old_scaled_sum_sqgrad = old.getSumOfSquaredGradients();
            old_sum_squared_delta_x = old.getSumOfSquaredDeltaX();
        }

        float new_scaled_sum_sq_grad = (decay * old_scaled_sum_sqgrad) + ((1.f - decay) * g_g);
        float dx = (float) Math.sqrt((old_sum_squared_delta_x + eps)
                / (old_scaled_sum_sqgrad * scaling + eps))
                * gradient;
        float new_sum_squared_delta_x = (decay * old_sum_squared_delta_x)
                + ((1.f - decay) * dx * dx);
        float new_w = old_w + (dx * xi);
        return new WeightValueParamsF2(new_w, new_scaled_sum_sq_grad, new_sum_squared_delta_x);
    }

}
