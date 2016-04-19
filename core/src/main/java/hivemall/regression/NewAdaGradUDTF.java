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
import hivemall.model.FeatureValue;
import hivemall.model.Solver.SolverType;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * ADAGRAD algorithm with element-wise adaptive learning rates.
 */
public final class NewAdaGradUDTF extends RegressionBaseUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException("NewAdagradUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, float target [, constant string options]");
        }

        StructObjectInspector oi = super.initialize(argOIs);
        model.configureParams(true, false, false);
        return oi;
    }

    @Override
    protected SolverType getSolverType() {
        return SolverType.AdaGrad;
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
        if (cl.hasOption("eta")) {
            solverOptions.put("eta", cl.getOptionValue("eta"));
        }
        if (cl.hasOption("eps")) {
            solverOptions.put("eps", cl.getOptionValue("eps"));
        }
        if (cl.hasOption("scale")) {
            solverOptions.put("scale", cl.getOptionValue("scale"));
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
        model.updateWeight(features, gradient);
    }

}
