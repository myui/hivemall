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

import hivemall.fm.FactorizationMachineModel.VInitScheme;
import hivemall.utils.codec.Base91;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.lang.Primitives;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * Field-aware Factorization Machines.
 * 
 * @link https://www.csie.ntu.edu.tw/~cjlin/libffm/
 */
@Description(
        name = "train_ffm",
        value = "_FUNC_(array<string> x, double y [, const string options]) - Returns a prediction model")
public final class FieldAwareFactorizationMachineUDTF extends FactorizationMachineUDTF {

    private boolean _globalBias;
    private boolean _linearCoeff;

    private FFMStringFeatureMapModel _model;
    private List<String> _fieldList;

    public FieldAwareFactorizationMachineUDTF() {
        super();
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("w0", "global_bias", false,
            "Whether to include global bias term w0 [default: OFF]");
        opts.addOption("w_i", "linear_coeff", false,
            "Whether to include linear term [default: OFF]");
        // adagrad
        opts.addOption("disable_adagrad", false,
            "Whether to use AdaGrad for tuning learning rate [default: ON]");
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
        this._globalBias = cl.hasOption("global_bias");
        this._linearCoeff = cl.hasOption("linear_coeff");
        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        StructObjectInspector oi = super.initialize(argOIs);
        this._fieldList = new ArrayList<String>();
        return oi;
    }

    @Override
    protected StructObjectInspector getOutputOI() {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("model_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        fieldNames.add("model");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    protected FFMStringFeatureMapModel initModel(@Nullable CommandLine cl) {
        float lambda0 = 0.01f;
        double sigma = 0.1d;
        double min_target = Double.MIN_VALUE, max_target = Double.MAX_VALUE;
        String vInitOpt = null;
        float maxInitValue = 1.f;
        double initStdDev = 0.1d;
        if (cl != null) {
            // FFM hyperparameters
            lambda0 = Primitives.parseFloat(cl.getOptionValue("lambda0"), lambda0);
            sigma = Primitives.parseDouble(cl.getOptionValue("sigma"), sigma);
            // Hyperparameter for regression
            min_target = Primitives.parseDouble(cl.getOptionValue("min_target"), min_target);
            max_target = Primitives.parseDouble(cl.getOptionValue("max_target"), max_target);
            // V initialization
            vInitOpt = cl.getOptionValue("init_v");
            maxInitValue = Primitives.parseFloat(cl.getOptionValue("max_init_value"), 1.f);
            initStdDev = Primitives.parseDouble(cl.getOptionValue("min_init_stddev"), 0.1d);
        }
        VInitScheme vInit = VInitScheme.resolve(vInitOpt);
        vInit.setMaxInitValue(maxInitValue);
        initStdDev = Math.max(initStdDev, 1.0d / _factor);
        vInit.setInitStdDev(initStdDev);
        vInit.initRandom(_factor, _seed);

        // adagrad
        boolean useAdaGrad = !cl.hasOption("disable_adagrad");
        float eta0_V = Primitives.parseFloat(cl.getOptionValue("eta0_V"), 1.f);
        float eps = Primitives.parseFloat(cl.getOptionValue("eps"), 1.f);
        float scaling = Primitives.parseFloat(cl.getOptionValue("scale"), 100f);

        FFMStringFeatureMapModel model = new FFMStringFeatureMapModel(_classification, _factor,
            lambda0, sigma, _seed, min_target, max_target, _etaEstimator, vInit, useAdaGrad,
            eta0_V, eps, scaling);
        this._model = model;
        return model;
    }

    @Override
    public void train(@Nonnull Feature[] x, double y, boolean adaptiveRegularization)
            throws HiveException {
        _model.check(x);
        try {
            trainTheta(x, y);
        } catch (Exception ex) {
            throw new HiveException("Exception caused in the " + _t + "-th call of train()", ex);
        }
    }

    @Override
    protected void trainTheta(@Nonnull Feature[] x, double y) throws HiveException {
        final float eta_t = _etaEstimator.eta(_t);

        final double p = _model.predict(x);
        final double lossGrad = _model.dloss(p, y);

        double loss = _lossFunction.loss(p, y);
        _cvState.incrLoss(loss);

        // w0 update
        if (_globalBias) {
            _model.updateW0(lossGrad, eta_t);
        }

        // wi update
        if (_linearCoeff) {
            for (int i = 0; i < x.length; i++) {
                _model.updateWi(lossGrad, x[i], eta_t);
            }
        }

        // ViFf update
        final List<String> fieldList = getFieldList(x);
        // sumVfX[i as in index for x][index for field list][index for factorized dimension]
        final double[][][] sumVfx = _model.sumVfX(x, fieldList);
        for (int i = 0; i < x.length; i++) {
            for (int fieldIndex = 0, size = fieldList.size(); fieldIndex < size; fieldIndex++) {
                final String field = fieldList.get(fieldIndex);
                for (int f = 0, k = _factor; f < k; f++) {
                    _model.updateV(lossGrad, x[i], field, f, sumVfx[i][fieldIndex][f], _t);
                }
            }
        }
        fieldList.clear();
    }

    @Nonnull
    private List<String> getFieldList(@Nonnull Feature[] x) {
        for (Feature e : x) {
            String field = e.getField();
            _fieldList.add(field);
        }
        return _fieldList;
    }

    @Override
    protected StringFeature instantiateFeature(@Nonnull ByteBuffer input) {
        return new StringFeature(input);
    }

    @Override
    public void close() throws HiveException {
        super.close();
        // help GC
        this._model = null;
        this._fieldList = null;
    }

    @Override
    protected void forwardModel() throws HiveException {
        Text modelId = new Text();
        Text modelObj = new Text();
        Object[] forwardObjs = new Object[] {modelId, modelObj};

        String taskId = HadoopUtils.getUniqueTaskIdString();
        modelId.set(taskId);

        FFMPredictionModel predModel = _model.toPredictionModel();
        this._model = null; // help GC

        byte[] serialized;
        try {
            serialized = predModel.serialize();
        } catch (IOException e) {
            throw new HiveException(e);
        }
        serialized = Base91.encode(serialized);
        modelObj.set(serialized);

        forward(forwardObjs);
    }

}
