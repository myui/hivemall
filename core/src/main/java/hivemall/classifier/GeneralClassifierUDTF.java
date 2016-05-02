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
package hivemall.classifier;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import hivemall.optimizer.LossFunctions;
import hivemall.model.FeatureValue;

/**
 * A general classifier class with replaceable optimization functions.
 */
public class GeneralClassifierUDTF extends BinaryOnlineClassifierUDTF {

    protected final Map<String, String> optimizerOptions;

    public GeneralClassifierUDTF() {
        this.optimizerOptions = new HashMap<String, String>();
        // Set default values
        optimizerOptions.put("optimizer", "adagrad");
        optimizerOptions.put("eta", "fixed");
        optimizerOptions.put("eta0", "1.0");
        optimizerOptions.put("regularization", "RDA");
        optimizerOptions.put("lambda", "1e-6");
        optimizerOptions.put("scale", "100.0");
        optimizerOptions.put("lambda", "1.0");
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(
                    this.getClass().getSimpleName()
                  + " takes 2 or 3 arguments: List<Text|Int|BitInt> features, int label "
                  + "[, constant string options]");
        }
        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("optimizer", "opt", true, "Optimizer to update weights [default: adagrad+rda]");
        opts.addOption("eta", "eta0", true, "Initial learning rate [default 1.0]");
        opts.addOption("lambda", true, "Lambda value of RDA [default: 1e-6f]");
        opts.addOption("scale", true, "Scaling factor for cumulative weights [100.0]");
        opts.addOption("regularization", "reg", true, "Regularization type [default not-defined]");
        opts.addOption("lambda", true, "Regularization term on weights [default 1.0]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);
        assert(cl != null);
        if(cl != null) {
            for(final String arg : cl.getArgs()) {
                optimizerOptions.put(arg, cl.getOptionValue(arg));
            }
        }
        return cl;
    }

    @Override
    protected Map<String, String> getOptimzierOptions() {
        return optimizerOptions;
    }

    @Override
    protected void train(@Nonnull final FeatureValue[] features, final int label) {
        float predicted = predict(features);
        update(features, label > 0 ? 1.f : -1.f, predicted);
    }

    @Override
    protected void update(@Nonnull final FeatureValue[] features, final float label,
            final float predicted) {
        if(is_mini_batch) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName() + " supports no `is_mini_batch` mode");
        } else {
            float loss = LossFunctions.hingeLoss(predicted, label);
            if(loss <= 0.f) {
                return;
            }
            for(FeatureValue f : features) {
                Object feature = f.getFeature();
                float xi = f.getValueAsFloat();
                float weight = model.getWeight(feature);
                float new_weight = optimizerImpl.computeUpdatedValue(feature, weight, -label * xi);
                model.setWeight(feature, new_weight);
            }
            optimizerImpl.proceedStep();
        }
    }

}
