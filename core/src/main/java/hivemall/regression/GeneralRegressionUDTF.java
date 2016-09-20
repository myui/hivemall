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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import hivemall.optimizer.LossFunctions;
import hivemall.model.FeatureValue;

/**
 * A general regression class with replaceable optimization functions.
 */
public class GeneralRegressionUDTF extends RegressionBaseUDTF {

    protected final Map<String, String> optimizerOptions;

    public GeneralRegressionUDTF() {
        super(true); // This enables new model interfaces
        this.optimizerOptions = new HashMap<String, String>();
        // Set default values
        optimizerOptions.put("optimizer", "adadelta");
        optimizerOptions.put("eta", "fixed");
        optimizerOptions.put("eta0", "1.0");
        optimizerOptions.put("t", "10000");
        optimizerOptions.put("power_t", "0.1");
        optimizerOptions.put("eps", "1e-6");
        optimizerOptions.put("rho", "0.95");
        optimizerOptions.put("scale", "100.0");
        optimizerOptions.put("lambda", "1.0");
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(
                    this.getClass().getSimpleName()
                  + " takes 2 or 3 arguments: List<Text|Int|BitInt> features, float target "
                  + "[, constant string options]");
        }
        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("optimizer", "opt", true, "Optimizer to update weights [default: adadelta]");
        opts.addOption("eta", true, " ETA estimator to compute delta [default: fixed]");
        opts.addOption("eta0", true, "Initial learning rate [default 1.0]");
        opts.addOption("t", "total_steps", true, "Total of n_samples * epochs time steps [default: 10000]");
        opts.addOption("power_t", true, "Exponent for inverse scaling learning rate [default 0.1]");
        opts.addOption("eps", true, "Denominator value of AdaDelta/AdaGrad [default 1e-6]");
        opts.addOption("rho", "decay", true, "Decay rate [default 0.95]");
        opts.addOption("scale", true, "Scaling factor for cumulative weights [100.0]");
        opts.addOption("regularization", "reg", true, "Regularization type [default not-defined]");
        opts.addOption("lambda", true, "Regularization term on weights [default 1.0]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);
        if(cl != null) {
            for(final Option opt: cl.getOptions()) {
                optimizerOptions.put(opt.getOpt(), opt.getValue());
            }
        }
        return cl;
    }

    @Override
    protected Map<String, String> getOptimzierOptions() {
        return optimizerOptions;
    }

    @Override
    protected final void checkTargetValue(final float target) throws UDFArgumentException {
        if(target < 0.f || target > 1.f) {
            throw new UDFArgumentException("target must be in range 0 to 1: " + target);
        }
    }

    @Override
    protected void update(@Nonnull final FeatureValue[] features, final float target,
            final float predicted) {
        if(is_mini_batch) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName() + " supports no `is_mini_batch` mode");
        } else {
            float loss = LossFunctions.logisticLoss(target, predicted);
            for(FeatureValue f : features) {
                Object feature = f.getFeature();
                float xi = f.getValueAsFloat();
                float weight = model.getWeight(feature);
                float new_weight = optimizerImpl.computeUpdatedValue(feature, weight, -loss * xi);
                model.setWeight(feature, new_weight);
            }
            optimizerImpl.proceedStep();
        }
    }

}
