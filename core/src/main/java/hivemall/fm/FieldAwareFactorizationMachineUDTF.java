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

import hivemall.fm.FMHyperParameters.FFMHyperParameters;
import hivemall.utils.collections.DoubleArray3D;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.hadoop.Text3;
import hivemall.utils.lang.NumberUtils;

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

    // ----------------------------------------
    // Learning hyper-parameters/options
    private boolean _globalBias;
    private boolean _linearCoeff;

    private int _numFeatures;
    private int _numFields;
    // ----------------------------------------

    private FFMStringFeatureMapModel _ffmModel;

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
        opts.addOption("l1_v", "L1_V", true, "L1 regularization value for AdaGrad [default: 0.01]");
        return opts;
    }

    @Override
    protected boolean isAdaptiveRegularizationSupported() {
        return false;
    }

    @Override
    protected FFMHyperParameters newHyperParameters() {
        return new FFMHyperParameters();
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = super.processOptions(argOIs);

        FFMHyperParameters params = (FFMHyperParameters) _params;
        if (params.parseFeatureAsInt) {
            throw new UDFArgumentException("int_feature option is not supported yet");
        }

        this._globalBias = params.globalBias;
        this._linearCoeff = params.linearCoeff;
        this._numFeatures = params.numFeatures;
        this._numFields = params.numFields;

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        StructObjectInspector oi = super.initialize(argOIs);

        this._fieldList = new IntArrayList();
        return oi;
    }

    @Override
    protected StructObjectInspector getOutputOI(@Nonnull FMHyperParameters params) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("model_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        fieldNames.add("model");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    protected FFMStringFeatureMapModel initModel(@Nullable CommandLine cl,
            @Nonnull FMHyperParameters params) throws UDFArgumentException {
        FFMHyperParameters ffmParams = (FFMHyperParameters) params;

        FFMStringFeatureMapModel model = new FFMStringFeatureMapModel(ffmParams);
        this._ffmModel = model;
        return model;
    }

    @Override
    protected Feature[] parseFeatures(@Nonnull final Object arg) throws HiveException {
        return Feature.parseFFMFeatures(arg, _xOI, _probes, _numFeatures, _numFields);
    }

    @Override
    public void train(@Nonnull final Feature[] x, final double y,
            final boolean adaptiveRegularization) throws HiveException {
        _ffmModel.check(x);
        try {
            trainTheta(x, y);
        } catch (Exception ex) {
            throw new HiveException("Exception caused in the " + _t + "-th call of train()", ex);
        }
    }

    @Override
    protected void trainTheta(@Nonnull final Feature[] x, final double y) throws HiveException {
        final float eta_t = _etaEstimator.eta(_t);

        final double p = _ffmModel.predict(x);
        final double lossGrad = _ffmModel.dloss(p, y);

        double loss = _lossFunction.loss(p, y);
        _cvState.incrLoss(loss);

        // w0 update
        if (_globalBias) {
            _ffmModel.updateW0(lossGrad, eta_t);
        }

        // wi update
        if (_linearCoeff) {
            for (int i = 0; i < x.length; i++) {
                _ffmModel.updateWi(lossGrad, x[i], eta_t);
            }
        }

        // ViFf update
        final IntArrayList fieldList = getFieldList(x);
        // sumVfX[i as in index for x][index for field list][index for factorized dimension]
        final DoubleArray3D sumVfX = _ffmModel.sumVfX(x, fieldList, _sumVfX);
        for (int i = 0; i < x.length; i++) {
            final Feature x_i = x[i];
            for (int fieldIndex = 0, size = fieldList.size(); fieldIndex < size; fieldIndex++) {
                final int yField = fieldList.get(fieldIndex);
                for (int f = 0, k = _factor; f < k; f++) {
                    double sumViX = sumVfX.get(i, fieldIndex, f);
                    _ffmModel.updateV(lossGrad, x_i, yField, f, sumViX, _t);
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
        this._ffmModel = null;
    }

    @Override
    protected void forwardModel() throws HiveException {
        this._model = null;
        this._fieldList = null;
        this._sumVfX = null;

        Text modelId = new Text();
        String taskId = HadoopUtils.getUniqueTaskIdString();
        modelId.set(taskId);

        FFMPredictionModel predModel = _ffmModel.toPredictionModel();
        this._ffmModel = null; // help GC

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

        if (LOG.isInfoEnabled()) {
            LOG.info("Forwarding a serialized/compressed model '" + modelId + "' of size: "
                    + NumberUtils.prettySize(serialized.length));
        }

        Text modelObj = new Text3(serialized);
        serialized = null;
        Object[] forwardObjs = new Object[] {modelId, modelObj};

        forward(forwardObjs);
    }

}
