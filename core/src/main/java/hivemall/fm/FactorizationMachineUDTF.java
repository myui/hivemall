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

import hivemall.UDTFWithOptions;
import hivemall.common.ConversionState;
import hivemall.common.EtaEstimator;
import hivemall.common.LossFunctions;
import hivemall.common.LossFunctions.LossFunction;
import hivemall.common.LossFunctions.LossType;
import hivemall.fm.FMStringFeatureMapModel.Entry;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.FileUtils;
import hivemall.utils.io.NioStatefullSegment;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.math.MathUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Reporter;

@Description(
        name = "train_fm",
        value = "_FUNC_(array<string> x, double y [, const string options]) - Returns a prediction model")
public class FactorizationMachineUDTF extends UDTFWithOptions {
    private static final Log LOG = LogFactory.getLog(FactorizationMachineUDTF.class);
    private static final int INT_BYTES = Integer.SIZE / 8;

    protected ListObjectInspector _xOI;
    protected PrimitiveObjectInspector _yOI;

    /**
     * Probe for the input X
     */
    @Nullable
    protected Feature[] _probes;

    // ----------------------------------------
    // Learning hyper-parameters/options

    protected FMHyperParameters _params;

    protected boolean _classification;
    protected int _iterations;
    protected int _factors;
    protected boolean _parseFeatureAsInt;

    // adaptive regularization
    @Nullable
    protected Random _va_rand;
    protected float _validationRatio;
    protected int _validationThreshold;

    protected LossFunction _lossFunction;
    protected EtaEstimator _etaEstimator;
    protected ConversionState _cvState;

    // ----------------------------------------

    protected FactorizationMachineModel _model;

    /**
     * The number of training examples processed
     */
    protected long _t;

    // file IO
    private ByteBuffer _inputBuf;
    private NioStatefullSegment _fileIO;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("c", "classification", false, "Act as classification");
        opts.addOption("seed", true, "Seed value [default: -1 (random)]");
        opts.addOption("iters", "iterations", true, "The number of iterations [default: 1]");
        opts.addOption("p", "num_features", true, "The size of feature dimensions");
        opts.addOption("factor", "factors", true, "The number of the latent variables [default: 5]");
        opts.addOption("sigma", true, "The standard deviation for initializing V [default: 0.1]");
        opts.addOption("lambda0", "lambda", true,
            "The initial lambda value for regularization [default: 0.01]");
        opts.addOption("lambdaW0", "lambda_w0", true,
            "The initial lambda value for W0 regularization [default: 0.01]");
        opts.addOption("lambdaWi", "lambda_wi", true,
            "The initial lambda value for Wi regularization [default: 0.01]");
        opts.addOption("lambdaV", "lambda_v", true,
            "The initial lambda value for V regularization [default: 0.01]");
        // regression
        opts.addOption("min", "min_target", true, "The minimum value of target variable");
        opts.addOption("max", "max_target", true, "The maximum value of target variable");
        // learning rates
        opts.addOption("eta", true, "The initial learning rate");
        opts.addOption("eta0", true, "The initial learning rate [default 0.05]");
        opts.addOption("t", "total_steps", true, "The total number of training examples");
        opts.addOption("power_t", true,
            "The exponent for inverse scaling learning rate [default 0.1]");
        // conversion check
        opts.addOption("disable_cv", "disable_cvtest", false,
            "Whether to disable convergence check [default: OFF]");
        opts.addOption("cv_rate", "convergence_rate", true,
            "Threshold to determine convergence [default: 0.005]");
        // adaptive regularization
        if (isAdaptiveRegularizationSupported()) {
            opts.addOption("adareg", "adaptive_regularizaion", false,
                "Whether to enable adaptive regularization [default: OFF]");
            opts.addOption("va_ratio", "validation_ratio", true,
                "Ratio of training data used for validation [default: 0.05f]");
            opts.addOption("va_threshold", "validation_threshold", true,
                "Threshold to start validation. "
                        + "At least N training examples are used before validation [default: 1000]");
        }
        // initialization of V
        opts.addOption("init_v", true, "Initialization strategy of matrix V [random, gaussian]"
                + "(default: 'random' for regression / 'gaussian' for classification)");
        opts.addOption("maxval", "max_init_value", true,
            "The maximum initial value in the matrix V [default: 0.5]");
        opts.addOption("min_init_stddev", true,
            "The minimum standard deviation of initial matrix V [default: 0.1]");
        // feature representation
        opts.addOption("int_feature", "feature_as_integer", false,
            "Parse a feature as integer [default: OFF]");
        return opts;
    }

    protected boolean isAdaptiveRegularizationSupported() {
        return true;
    }

    @Override
    protected CommandLine processOptions(@Nonnull ObjectInspector[] argOIs)
            throws UDFArgumentException {
        final FMHyperParameters params = _params;

        CommandLine cl = null;
        if (argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);
            params.processOptions(cl);
        }

        this._classification = params.classification;
        this._iterations = params.iters;
        this._factors = params.factors;
        this._parseFeatureAsInt = params.parseFeatureAsInt;
        if (params.adaptiveReglarization) {
            this._va_rand = new Random(params.seed + 31L);
        }
        this._validationRatio = params.validationRatio;
        this._validationThreshold = params.validationThreshold;
        this._lossFunction = params.classification ? LossFunctions.getLossFunction(LossType.LogLoss)
                : LossFunctions.getLossFunction(LossType.SquaredLoss);
        this._etaEstimator = params.eta;
        this._cvState = new ConversionState(params.conversionCheck, params.convergenceRate);
        return cl;
    }

    @Nonnull
    protected FactorizationMachineModel getModel() {
        return _model;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(
                getClass().getSimpleName()
                        + " takes 2 or 3 arguments: array<string> x, double y [, CONSTANT STRING options]: "
                        + Arrays.toString(argOIs));
        }
        this._xOI = HiveUtils.asListOI(argOIs[0]);
        HiveUtils.validateFeatureOI(_xOI.getListElementObjectInspector());
        this._yOI = HiveUtils.asDoubleCompatibleOI(argOIs[1]);

        this._params = newHyperParameters();
        CommandLine cl = processOptions(argOIs);

        this._model = initModel(cl, _params);
        this._t = 0L;

        if (LOG.isInfoEnabled()) {
            LOG.info(_params);
        }

        return getOutputOI(_params);
    }

    @Nonnull
    protected FMHyperParameters newHyperParameters() {
        return new FMHyperParameters();
    }

    @Nonnull
    protected StructObjectInspector getOutputOI(@Nonnull FMHyperParameters params) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("feature");
        if (params.parseFeatureAsInt) {
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        } else {
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        fieldNames.add("W_i");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        fieldNames.add("V_if");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Nonnull
    protected FactorizationMachineModel initModel(@Nullable CommandLine cl,
            @Nonnull FMHyperParameters params) throws UDFArgumentException {
        if (params.parseFeatureAsInt) {
            if (params.numFeatures == -1) {
                return new FMIntFeatureMapModel(params);
            } else {
                return new FMArrayModel(params);
            }
        } else {
            return new FMStringFeatureMapModel(params);
        }
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Feature[] x = parseFeatures(args[0]);
        if (x == null) {
            return;
        }
        this._probes = x;

        double y = PrimitiveObjectInspectorUtils.getDouble(args[1], _yOI);
        if (_classification) {
            y = (y > 0.d) ? 1.d : -1.d;
        }

        ++_t;
        recordTrain(x, y);
        boolean adaptiveRegularization = (_va_rand != null) && _t >= _validationThreshold;
        train(x, y, adaptiveRegularization);
    }

    @Nullable
    protected Feature[] parseFeatures(@Nonnull final Object arg) throws HiveException {
        return Feature.parseFeatures(arg, _xOI, _probes, _parseFeatureAsInt);
    }

    protected void recordTrain(@Nonnull final Feature[] x, final double y) throws HiveException {
        if (_iterations <= 1) {
            return;
        }

        ByteBuffer inputBuf = _inputBuf;
        NioStatefullSegment dst = _fileIO;
        if (inputBuf == null) {
            final File file;
            try {
                file = File.createTempFile("hivemall_fm", ".sgmt");
                file.deleteOnExit();
                if (!file.canWrite()) {
                    throw new UDFArgumentException("Cannot write a temporary file: "
                            + file.getAbsolutePath());
                }
                LOG.info("Record training examples to a file: " + file.getAbsolutePath());
            } catch (IOException ioe) {
                throw new UDFArgumentException(ioe);
            } catch (Throwable e) {
                throw new UDFArgumentException(e);
            }

            this._inputBuf = inputBuf = ByteBuffer.allocateDirect(1024 * 1024); // 1 MiB
            this._fileIO = dst = new NioStatefullSegment(file, false);
        }

        int xBytes = Feature.requiredBytes(x);
        int recordBytes = (Integer.SIZE + Double.SIZE) / 8 + xBytes;
        int requiredBytes = (Integer.SIZE / 8) + recordBytes;
        int remain = inputBuf.remaining();
        if (remain < requiredBytes) {
            writeBuffer(inputBuf, dst);
        }

        inputBuf.putInt(recordBytes);
        inputBuf.putInt(x.length);
        for (Feature f : x) {
            f.writeTo(inputBuf);
        }
        inputBuf.putDouble(y);
    }

    private static void writeBuffer(@Nonnull ByteBuffer srcBuf, @Nonnull NioStatefullSegment dst)
            throws HiveException {
        srcBuf.flip();
        try {
            dst.write(srcBuf);
        } catch (IOException e) {
            throw new HiveException("Exception causes while writing a buffer to file", e);
        }
        srcBuf.clear();
    }

    public void train(@Nonnull final Feature[] x, final double y,
            final boolean adaptiveRegularization) throws HiveException {
        _model.check(x);

        try {
            if (adaptiveRegularization) {
                assert (_va_rand != null);
                final float rnd = _va_rand.nextFloat();
                if (rnd < _validationRatio) {
                    trainLambda(x, y); // adaptive regularization
                } else {
                    trainTheta(x, y);
                }
            } else {
                trainTheta(x, y);
            }
        } catch (Exception ex) {
            throw new HiveException("Exception caused in the " + _t + "-th call of train()", ex);
        }
    }

    /**
     * Update model parameters
     */
    protected void trainTheta(final Feature[] x, final double y) throws HiveException {
        final float eta = _etaEstimator.eta(_t);

        final double p = _model.predict(x);
        final double lossGrad = _model.dloss(p, y);
        double loss = _lossFunction.loss(p, y);
        _cvState.incrLoss(loss);

        if (MathUtils.closeToZero(lossGrad)) {
            return;
        }

        // w0 update
        _model.updateW0(lossGrad, eta);

        final double[] sumVfx = _model.sumVfX(x);
        for (Feature xi : x) {
            // wi update
            _model.updateWi(lossGrad, xi, eta);
            for (int f = 0, k = _factors; f < k; f++) {
                // Vif update
                _model.updateV(lossGrad, xi, f, sumVfx[f], eta);
            }
        }
    }

    /**
     * Update regularization parameters `lambda` as follows:
     * 
     * <pre>
     *      grad_lambdaw0 = (grad l(p,y)) * (-2 * alpha * w_0)
     *      grad_lambdawg = (grad l(p,y)) * (-2 * alpha * (\sum_{l \in group(g)} x_l * w_l))
     *      grad_lambdafg = (grad l(p,y)) * (-2 * alpha * (\sum_{l} x_l * v'_lf) * \sum_{l \in group(g)} x_l * v_lf) - \sum_{l \in group(g)} x^2_l * v_lf * v'_lf)
     * </pre>
     */
    protected void trainLambda(final Feature[] x, final double y) throws HiveException {
        final float eta = _etaEstimator.eta(_t);
        final double p = _model.predict(x);
        final double lossGrad = _model.dloss(p, y);

        _model.updateLambdaW0(lossGrad, eta);
        _model.updateLambdaW(x, lossGrad, eta);
        _model.updateLambdaV(x, lossGrad, eta);
    }

    @Override
    public void close() throws HiveException {
        this._probes = null;

        if (_t == 0) {
            this._model = null;
            return;
        }
        if (_iterations > 1) {
            runTrainingIteration(_iterations);
        }

        final int P = _model.getSize();
        if (P <= 0) {
            LOG.warn("Model size P was less than zero: " + P);
            this._model = null;
            return;
        }

        forwardModel();
        this._model = null;
    }

    protected void forwardModel() throws HiveException {
        if (_parseFeatureAsInt) {
            forwardAsIntFeature(_model, _factors);
        } else {
            FMStringFeatureMapModel strModel = (FMStringFeatureMapModel) _model;
            forwardAsStringFeature(strModel, _factors);
        }
    }

    private void forwardAsIntFeature(@Nonnull final FactorizationMachineModel model,
            final int factors) throws HiveException {
        final IntWritable f_idx = new IntWritable(0);
        final FloatWritable f_Wi = new FloatWritable(0.f);
        final FloatWritable[] f_Vi = HiveUtils.newFloatArray(factors, 0.f);

        final Object[] forwardObjs = new Object[3];
        forwardObjs[0] = f_idx;
        forwardObjs[1] = f_Wi;
        forwardObjs[2] = null;
        // W0
        f_idx.set(0);
        f_Wi.set(model.getW0());
        // V0 is null
        forward(forwardObjs);

        // Wi, Vif (i starts from 1..P)
        forwardObjs[2] = Arrays.asList(f_Vi);

        for (int i = model.getMinIndex(), maxIdx = model.getMaxIndex(); i <= maxIdx; i++) {
            final float[] vi = model.getV(i);
            if (vi == null) {
                continue;
            }
            f_idx.set(i);
            // set Wi
            final float w = model.getW(i);
            f_Wi.set(w);
            // set Vif
            for (int f = 0; f < factors; f++) {
                float v = vi[f];
                f_Vi[f].set(v);
            }
            forward(forwardObjs);
        }
    }

    private void forwardAsStringFeature(@Nonnull final FMStringFeatureMapModel model,
            final int factors) throws HiveException {
        final Text feature = new Text();
        final FloatWritable f_Wi = new FloatWritable(0.f);
        final FloatWritable[] f_Vi = HiveUtils.newFloatArray(factors, 0.f);

        final Object[] forwardObjs = new Object[3];
        forwardObjs[0] = feature;
        forwardObjs[1] = f_Wi;
        forwardObjs[2] = null;
        // W0
        feature.set("0");
        f_Wi.set(model.getW0());
        // V0 is null
        forward(forwardObjs);

        // Wi, Vif (i starts from 1..P)
        forwardObjs[2] = Arrays.asList(f_Vi);

        final IMapIterator<String, Entry> itor = model.entries();
        while (itor.next() != -1) {
            String i = itor.getKey();
            assert (i != null);
            // set i
            feature.set(i);
            Entry entry = itor.getValue();
            // set Wi
            f_Wi.set(entry.W);
            // set Vif
            final float[] Vi = entry.Vf;
            for (int f = 0; f < factors; f++) {
                float v = Vi[f];
                f_Vi[f].set(v);
            }
            forward(forwardObjs);
        }
    }

    protected void runTrainingIteration(int iterations) throws HiveException {
        final ByteBuffer inputBuf = this._inputBuf;
        final NioStatefullSegment fileIO = this._fileIO;
        assert (inputBuf != null);
        assert (fileIO != null);
        final long numTrainingExamples = _t;
        final boolean adaregr = _va_rand != null;

        final Reporter reporter = getReporter();
        final Counter iterCounter = (reporter == null) ? null : reporter.getCounter(
            "hivemall.fm.FactorizationMachines$Counter", "iteration");

        try {
            if (fileIO.getPosition() == 0L) {// run iterations w/o temporary file
                if (inputBuf.position() == 0) {
                    return; // no training example
                }
                inputBuf.flip();

                int iter = 2;
                for (; iter <= iterations; iter++) {
                    reportProgress(reporter);
                    setCounterValue(iterCounter, iter);

                    while (inputBuf.remaining() > 0) {
                        int bytes = inputBuf.getInt();
                        assert (bytes > 0) : bytes;
                        int xLength = inputBuf.getInt();
                        final Feature[] x = new Feature[xLength];
                        for (int j = 0; j < xLength; j++) {
                            x[j] = instantiateFeature(inputBuf);
                        }
                        double y = inputBuf.getDouble();
                        // invoke train
                        ++_t;
                        train(x, y, adaregr);
                    }
                    if (_cvState.isConverged(iter, numTrainingExamples)) {
                        break;
                    }
                    inputBuf.rewind();
                }
                LOG.info("Performed " + Math.min(iter, iterations) + " iterations of "
                        + NumberUtils.formatNumber(numTrainingExamples)
                        + " training examples on memory (thus " + NumberUtils.formatNumber(_t)
                        + " training updates in total) ");
            } else {// read training examples in the temporary file and invoke train for each example

                // write training examples in buffer to a temporary file
                if (inputBuf.remaining() > 0) {
                    writeBuffer(inputBuf, fileIO);
                }
                try {
                    fileIO.flush();
                } catch (IOException e) {
                    throw new HiveException("Failed to flush a file: "
                            + fileIO.getFile().getAbsolutePath(), e);
                }
                if (LOG.isInfoEnabled()) {
                    File tmpFile = fileIO.getFile();
                    LOG.info("Wrote " + numTrainingExamples
                            + " records to a temporary file for iterative training: "
                            + tmpFile.getAbsolutePath() + " (" + FileUtils.prettyFileSize(tmpFile)
                            + ")");
                }

                // run iterations
                int iter = 2;
                for (; iter <= iterations; iter++) {
                    setCounterValue(iterCounter, iter);

                    inputBuf.clear();
                    fileIO.resetPosition();
                    while (true) {
                        reportProgress(reporter);
                        // TODO prefetch
                        // writes training examples to a buffer in the temporary file
                        final int bytesRead;
                        try {
                            bytesRead = fileIO.read(inputBuf);
                        } catch (IOException e) {
                            throw new HiveException("Failed to read a file: "
                                    + fileIO.getFile().getAbsolutePath(), e);
                        }
                        if (bytesRead == 0) { // reached file EOF
                            break;
                        }
                        assert (bytesRead > 0) : bytesRead;

                        // reads training examples from a buffer
                        inputBuf.flip();
                        int remain = inputBuf.remaining();
                        if (remain < INT_BYTES) {
                            throw new HiveException("Illegal file format was detected");
                        }
                        while (remain >= INT_BYTES) {
                            int pos = inputBuf.position();
                            int recordBytes = inputBuf.getInt();
                            remain -= INT_BYTES;
                            if (remain < recordBytes) {
                                inputBuf.position(pos);
                                break;
                            }

                            final int xLength = inputBuf.getInt();
                            final Feature[] x = new Feature[xLength];
                            for (int j = 0; j < xLength; j++) {
                                x[j] = instantiateFeature(inputBuf);
                            }
                            double y = inputBuf.getDouble();

                            // invoke training
                            ++_t;
                            train(x, y, adaregr);

                            remain -= recordBytes;
                        }
                        inputBuf.compact();
                    }
                    if (_cvState.isConverged(iter, numTrainingExamples)) {
                        break;
                    }
                }
                LOG.info("Performed " + Math.min(iter, iterations) + " iterations of "
                        + NumberUtils.formatNumber(numTrainingExamples)
                        + " training examples on a secondary storage (thus "
                        + NumberUtils.formatNumber(_t) + " training updates in total)");
            }
        } finally {
            // delete the temporary file and release resources
            try {
                fileIO.close(true);
            } catch (IOException e) {
                throw new HiveException("Failed to close a file: "
                        + fileIO.getFile().getAbsolutePath(), e);
            }
            this._inputBuf = null;
            this._fileIO = null;
        }
    }

    @Nonnull
    protected Feature instantiateFeature(@Nonnull final ByteBuffer input) {
        if (_parseFeatureAsInt) {
            return new IntFeature(input);
        } else {
            return new StringFeature(input);
        }
    }

}
