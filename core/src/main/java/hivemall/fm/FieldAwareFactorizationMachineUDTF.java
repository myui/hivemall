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
        return cl;
    }

    @Override
    protected FactorizationMachineModel initModel() {
        return new FFMStringFeatureMapModel(_classification, _factor, _lambda0, _sigma, _seed,
            _min_target, _max_target, _etaEstimator, _vInit);
    }

    @Override
    protected void trainTheta(Feature[] x, double y) throws HiveException {
        final FieldAwareFactorizationMachineModel m = (FieldAwareFactorizationMachineModel) getModel();
        final float eta = _etaEstimator.eta(_t);

        final double p = m.predict(x);
        final double lossGrad = m.dloss(p, y);

        double loss = _lossFunction.loss(p, y);
        _cvState.incrLoss(loss);

        // w0 update
        if (globalBias) {
            m.updateW0(lossGrad, eta);
        }

        // wi update
        if (linearCoeff) {
            for (int i = 0; i < x.length; i++) {
                m.updateWi(lossGrad, x[i], eta);
            }
        }

        // ViFf update
        final List<String> fieldList = getFieldList(x);
        // sumVfX[i as in index for x][index for field list][index for factorized dimension]
        final double[][][] sumVfx = m.sumVfX(x, fieldList);
        for (int i = 0; i < x.length; i++) {
            for (int fieldIndex = 0, size = fieldList.size(); fieldIndex < size; fieldIndex++) {
                for (int f = 0, k = _factor; f < k; f++) {
                    // Vif update
                    m.updateV(lossGrad, x[i], f, sumVfx[i][fieldIndex][f], eta,
                        fieldList.get(fieldIndex));
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
