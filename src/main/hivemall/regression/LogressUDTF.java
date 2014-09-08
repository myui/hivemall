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
import hivemall.common.LossFunctions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public final class LogressUDTF extends OnlineRegressionUDTF {

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
        
        this.etaEstimator = EtaEstimator.get(cl);
        return cl;
    }

    @Override
    protected void checkTargetValue(final float target) throws UDFArgumentException {
        if(target < 0.f || target > 1.f) {
            throw new UDFArgumentException("target must be in range 0 to 1: " + target);
        }
    }

    @Override
    protected float computeUpdate(final float target, final float predicted) {
        float eta = etaEstimator.eta(count);
        float gradient = LossFunctions.logisticLoss(target, predicted);
        return eta * gradient;
    }

}
