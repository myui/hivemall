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

import hivemall.common.EtaEstimator;
import hivemall.common.EtaEstimator.InvscalingEtaEstimator;
import hivemall.common.EtaEstimator.SimpleEtaEstimator;
import hivemall.utils.MathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class LogressUDTF extends OnlineRegressionUDTF {

    private EtaEstimator etaEstimator;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("LogressUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, float target [, constant string options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("t", "total_steps", true, "a total of n_samples * epochs time steps");
        opts.addOption("power_t", true, "The exponent for inverse scaling learning rate [default 0.1]");
        opts.addOption("eta0", true, "The initial learning rate [default 0.1]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);

        this.etaEstimator = getEtaEstimator(cl);
        return cl;
    }

    protected static EtaEstimator getEtaEstimator(CommandLine cl) throws UDFArgumentException {
        if(cl == null) {
            return new InvscalingEtaEstimator(0.1f, 0.1f);
        }

        float eta0 = Float.parseFloat(cl.getOptionValue("eta0", "0.1"));

        if(cl.hasOption("t")) {
            int t = Integer.parseInt(cl.getOptionValue("t"));
            return new SimpleEtaEstimator(eta0, t);
        }

        float power_t = Float.parseFloat(cl.getOptionValue("power_t", "0.1"));
        return new InvscalingEtaEstimator(eta0, power_t);
    }

    @Override
    protected void checkTargetValue(float target) throws UDFArgumentException {
        if(target < 0.f || target > 1.f) {
            throw new UDFArgumentException("target must be in range 0 to 1: " + target);
        }
    }

    @Override
    protected float dloss(float target, float predicted) {
        float eta = eta();
        float loss = computeLoss(target, predicted);
        return eta * loss;
    }

    private float eta() {
        return etaEstimator.eta(count);
    }

    private float computeLoss(float target, float predicted) {
        final float gradient;
        if(-100.d < predicted) {
            gradient = target - (float) MathUtils.sigmoid(predicted);
        } else {
            gradient = target;
        }
        return gradient;
    }

}
