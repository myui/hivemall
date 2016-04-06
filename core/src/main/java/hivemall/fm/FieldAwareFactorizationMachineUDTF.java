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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public final class FieldAwareFactorizationMachineUDTF extends FactorizationMachineUDTF {
    
    protected boolean constantTerm;
    protected boolean linearTerm;
    private ArrayList<Object> fieldList;

    public FieldAwareFactorizationMachineUDTF() {
        super();
        this.fieldList = new ArrayList<Object>();
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("const", "constant_term", true, "Whether to include constant bias term [default false]");
        opts.addOption("lin", "linear_term", true, "Whether to include linear term [default false]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);
        this.constantTerm = Boolean.valueOf(cl.getOptionValue("const", "false"));
        this.linearTerm = Boolean.valueOf(cl.getOptionValue("lin", "false"));
        return cl;
    }
    
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {//TODO _parseFeatureAsInt true will not work (IntFeature must be updated)
        StructObjectInspector result = super.initialize(argOIs);
        super.setModel(new FFMStringFeatureMapModel(_classification, _factor, _lambda0, _sigma,
                _seed, _min_target, _max_target, _etaEstimator, _vInit));
        return result;
    }

    @Override
    public void train(Feature[] x, double y, boolean adaptiveRegularization) throws HiveException {
        addField(x);
        //TODO support adaptiveRegularization
        // check
        getModel().check(x);
        try {
            trainTheta(x,y);
        } catch (Exception e) {
            throw new HiveException("Exception caused in the " + _t + "-th call of train()", e);
        }
        return;
    }

    private void addField(Feature[] x) {
        for (Feature e : x) {
            boolean exists = false;
            Object field = e.getField();
            for (Object s : fieldList) {
                if (field.equals(s)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                fieldList.add(field);
            }
        }
    }

    @Override
    protected void trainTheta(Feature[] x, double y) throws HiveException {
        FieldAwareFactorizationMachineModel m = (FieldAwareFactorizationMachineModel) getModel();
        final float eta = _etaEstimator.eta(_t);

        final double p = m.predict(x);
        final double lossGrad = m.dloss(p, y);

        double norm = calcNorm(x, m);
        assert(!Double.isNaN(norm));
        
        double loss = logLossWithRegularization(p, y, _lambda0, norm);
        _cvState.incrLoss(loss);

        // w0 update
        if (constantTerm) {
            m.updateW0(lossGrad, eta);
        }
        
        // wi update
        if (linearTerm) {
            for (int i = 0; i < x.length; ++i) {
                m.updateWi(lossGrad, x[i], eta);
            }
        }

        final double[][][] sumVfx = m.sumVfX(x, fieldList);//[i as in index for x][index for field list][index for factorized dimension]
        for (int i = 0; i < x.length; ++i) {
            for (int fieldIndex = 0; fieldIndex < fieldList.size(); ++fieldIndex) {
                for (int f = 0, k = _factor; f < k; ++f) {
                    // Vif update
                    //_model.updateV(x, lossGrad, i, f, eta);
                    m.updateV(lossGrad, x[i], f, sumVfx[i][fieldIndex][f], eta, fieldList.get(fieldIndex));
                }
            }
        }
    }

    //TODO how to move loss function with regularization into hivemall.common.LossFunctions? (does not fit into LossFunction interface due to num args)
    private double logLossWithRegularization(final double p, final double y, final float lambda, final double norm) {
        if(!(y == 1.f || y == -1.f)) {
            throw new IllegalArgumentException("target must be [+1,-1]: " + y);
        }//BinaryLoss.checkTarget(y);

        final double z = y * p;
        if(z > 18.f) {
            return Math.exp(-z);
        }
        if(z < -18.f) {
            return -z;
        }
        return Math.log(1.d + Math.exp(-z) + (lambda * norm / 2.d));
    }
    
    private double calcNorm(Feature[] x, FieldAwareFactorizationMachineModel m) {
        double norm = 0.d;
        // w0
        if(constantTerm) {
            norm = m.getW0();
            norm *= norm;
        }
        // W
        if(linearTerm) {
            for (Feature e : x) {
                float w = m.getW(e);
                double w2 = w * w;
                norm += w2;
            }
        }
        // V
        for (int f = 0, k = _factor; f < k; f++) {
            for (int i = 0; i < x.length; ++i) {
                for (int fieldIndex = 0; fieldIndex < fieldList.size(); ++fieldIndex) {
                    float vijf = m.getV(x[i], fieldList.get(fieldIndex), f);
                    norm += vijf*vijf;
                }
            }
        }
        return norm;
    }
}
