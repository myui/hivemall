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
package hivemall.fm;

import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public final class FieldAwareFactorizationMachineUDTF extends FactorizationMachineUDTF {

    private boolean globalBias;
    private boolean linearCoeff;

    // adagrad
    private boolean useAdaGrad;
    private float eta0_V;
    private float eps;
    private float scaling;

    private FieldAwareFactorizationMachineModel model;

    @Nonnull
    private final List<String> fieldList;

    public FieldAwareFactorizationMachineUDTF() {
        super();
        this.fieldList = new ArrayList<String>();
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("w0", "global_bias", false,
            "Whether to include global bias term w0 [default: OFF]");
        opts.addOption("w_i", "linear_coeff", false,
            "Whether to include linear term [default: OFF]");
        // adagrad
        opts.addOption("adagrad", false,
            "Whether to use AdaGrad for tuning learning rate [default: OFF]");
        opts.addOption("eta0_V", true, "The initial learning rate for V [default 1.0]");
        opts.addOption("eps", true, "A constant used in the denominator of AdaGrad [default 1.0]");
        opts.addOption("scale", true,
            "Internal scaling/descaling factor for cumulative weights [100]");
        return opts;
    }

    @Override
    protected boolean isAdaptiveRegularizationSupported() {
        return false;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        if (_parseFeatureAsInt) {
            throw new UDFArgumentException("int_feature option is not supported yet");
        }
        this.globalBias = cl.hasOption("global_bias");
        this.linearCoeff = cl.hasOption("linear_coeff");
        this.useAdaGrad = cl.hasOption("adagrad");
        this.eta0_V = Primitives.parseFloat(cl.getOptionValue("eta0_V"), 1.f);
        this.eps = Primitives.parseFloat(cl.getOptionValue("eps"), 1.f);
        this.scaling = Primitives.parseFloat(cl.getOptionValue("scale"), 100f);
        return cl;
    }

    @Override
    protected FieldAwareFactorizationMachineModel initModel() {
        FFMStringFeatureMapModel model = new FFMStringFeatureMapModel(_classification, _factor,
            _lambda0, _sigma, _seed, _min_target, _max_target, _etaEstimator, _vInit, useAdaGrad,
            eta0_V, eps, scaling);
        this.model = model;
        return model;
    }

    @Override
    protected void trainTheta(Feature[] x, double y) throws HiveException {
        final float eta_t = _etaEstimator.eta(_t);

        final double p = model.predict(x);
        final double lossGrad = model.dloss(p, y);

        double loss = _lossFunction.loss(p, y);
        _cvState.incrLoss(loss);

        // w0 update
        if (globalBias) {
            model.updateW0(lossGrad, eta_t);
        }

        // wi update
        if (linearCoeff) {
            for (int i = 0; i < x.length; i++) {
                model.updateWi(lossGrad, x[i], eta_t);
            }
        }

        // ViFf update
        final List<String> fieldList = getFieldList(x);
        // sumVfX[i as in index for x][index for field list][index for factorized dimension]
        final double[][][] sumVfx = model.sumVfX(x, fieldList);
        for (int i = 0; i < x.length; i++) {
            for (int fieldIndex = 0, size = fieldList.size(); fieldIndex < size; fieldIndex++) {
                String feild = fieldList.get(fieldIndex);
                for (int f = 0, k = _factor; f < k; f++) {
                    model.updateV(lossGrad, x[i], feild, f, sumVfx[i][fieldIndex][f], _t);
                }
            }
        }
        fieldList.clear();
    }

    @Nonnull
    private List<String> getFieldList(@Nonnull Feature[] x) {
        for (Feature e : x) {
            String field = e.getField();
            fieldList.add(field);
        }
        return fieldList;
    }
}
