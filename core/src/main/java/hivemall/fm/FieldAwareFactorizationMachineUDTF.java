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
import hivemall.utils.collections.DoubleArray3D;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Primitives;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    private static final Log LOG = LogFactory.getLog(FieldAwareFactorizationMachineUDTF.class);

    private boolean _globalBias;
    private boolean _linearCoeff;

    private int _numFeatures;
    private int _numFields;

    private FFMStringFeatureMapModel _model;
    private IntArrayList _fieldList;

    @Nullable
    private DoubleArray3D _sumVfX;

    public FieldAwareFactorizationMachineUDTF() {
        super();
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("all_terms", false,
            "Whether to include all terms (i.e., w0 and w_i) [default: OFF]");
        opts.addOption("w0", "global_bias", false,
            "Whether to include global bias term w0 [default: OFF]");
        opts.addOption("w_i", "linear_coeff", false,
            "Whether to include linear term [default: OFF]");
        // feature hashing
        opts.addOption("feature_hashing", true,
            "The number of bits for feature hashing in range [18,31] [default:21]");
        opts.addOption("num_fields", true, "The number of fields [default:1024]");
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
        if (cl.hasOption("all_terms")) {
            this._globalBias = true;
            this._linearCoeff = true;
        } else {
            this._globalBias = cl.hasOption("global_bias");
            this._linearCoeff = cl.hasOption("linear_coeff");
        }

        // feature hashing
        int hashbits = Primitives.parseInt(cl.getOptionValue("feature_hashing"),
            Feature.DEFAULT_FEATURE_BITS);
        if (hashbits < 18 || hashbits > 31) {
            throw new UDFArgumentException("-feature_hashing MUST be in range [18,31]: " + hashbits);
        }
        int numFeatures = 1 << hashbits;
        int numFields = Primitives.parseInt(cl.getOptionValue("num_fields"),
            Feature.DEFAULT_NUM_FIELDS);
        if (numFields <= 1) {
            throw new UDFArgumentException("-num_fields MUST be greater than 1: " + numFields);
        }
        this._numFeatures = numFeatures;
        this._numFields = numFields;

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        StructObjectInspector oi = super.initialize(argOIs);
        this._fieldList = new IntArrayList();
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
    protected FFMStringFeatureMapModel initModel(@Nullable CommandLine cl)
            throws UDFArgumentException {
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
            eta0_V, eps, scaling, _numFeatures, _numFields);
        this._model = model;
        return model;
    }

    @Override
    protected Feature[] parseFeatures(@Nonnull final Object arg) throws HiveException {
        return Feature.parseFFMFeatures(arg, _xOI, _probes, _numFeatures, _numFields);
    }

    @Override
    public void train(@Nonnull final Feature[] x, final double y,
            final boolean adaptiveRegularization) throws HiveException {
        _model.check(x);
        try {
            trainTheta(x, y);
        } catch (Exception ex) {
            throw new HiveException("Exception caused in the " + _t + "-th call of train()", ex);
        }
    }

    @Override
    protected void trainTheta(@Nonnull final Feature[] x, final double y) throws HiveException {
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
        final IntArrayList fieldList = getFieldList(x);
        // sumVfX[i as in index for x][index for field list][index for factorized dimension]
        final DoubleArray3D sumVfX = _model.sumVfX(x, fieldList, _sumVfX);
        for (int i = 0; i < x.length; i++) {
            final Feature x_i = x[i];
            for (int fieldIndex = 0, size = fieldList.size(); fieldIndex < size; fieldIndex++) {
                final int yField = fieldList.get(fieldIndex);
                for (int f = 0, k = _factor; f < k; f++) {
                    double sumViX = sumVfX.get(i, fieldIndex, f);
                    _model.updateV(lossGrad, x_i, yField, f, sumViX, _t);
                }
            }
        }

        // clean up per training instance caches
        sumVfX.clear();
        this._sumVfX = sumVfX;
        fieldList.clear();
    }

    @Nonnull
    private IntArrayList getFieldList(@Nonnull final Feature[] x) {
        for (Feature e : x) {
            int field = e.getField();
            _fieldList.add(field);
        }
        return _fieldList;
    }

    @Override
    protected IntFeature instantiateFeature(@Nonnull final ByteBuffer input) {
        return new IntFeature(input);
    }

    @Override
    public void close() throws HiveException {
        super.close();
        // help GC
        this._model = null;
        this._fieldList = null;
        this._sumVfX = null;
    }

    @Override
    protected void forwardModel() throws HiveException {
        this._fieldList = null;
        this._sumVfX = null;

        Text modelId = new Text();
        Text modelObj = new Text();
        Object[] forwardObjs = new Object[] {modelId, modelObj};

        String taskId = HadoopUtils.getUniqueTaskIdString();
        modelId.set(taskId);

        FFMPredictionModel predModel = _model.toPredictionModel();
        this._model = null; // help GC

        if (LOG.isInfoEnabled()) {
            LOG.info("Serializing a model '" + modelId + "'... Configured # features: "
                    + _numFeatures + ", Configured # fields: " + _numFields
                    + ", Actual # features: " + predModel.getActualNumFeatures()
                    + ", Estimated uncompressed bytes: "
                    + NumberUtils.prettySize(predModel.approxBytesConsumed()));
        }

        byte[] serialized;
        try {
            serialized = predModel.serialize();
            predModel = null;
        } catch (IOException e) {
            throw new HiveException("Failed to serialize a model", e);
        }

        modelObj.set(serialized);
        if (LOG.isInfoEnabled()) {
            LOG.info("Forwarding a serialized/compressed model '" + modelId + "' of size: "
                    + NumberUtils.prettySize(serialized.length));
        }
        serialized = null;

        forward(forwardObjs);
    }

}
