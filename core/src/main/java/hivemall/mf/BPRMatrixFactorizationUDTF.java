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
package hivemall.mf;

import hivemall.UDTFWithOptions;
import hivemall.common.ConversionState;
import hivemall.common.EtaEstimator;
import hivemall.mf.FactorizedModel.RankInitScheme;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.FileUtils;
import hivemall.utils.io.NioFixedSegment;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Primitives;
import hivemall.utils.math.MathUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import javax.annotation.Nonnegative;
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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Reporter;

/**
 * Bayesian Personalized Ranking from Implicit Feedback.
 */
@Description(name = "train_bprmf",
        value = "_FUNC_(INT user, INT posItem, INT negItem [, String options])"
                + " - Returns a relation <INT i, FLOAT Pi, FLOAT Qi [, FLOAT Bi]>")
public final class BPRMatrixFactorizationUDTF extends UDTFWithOptions implements RatingInitilizer {
    private static final Log LOG = LogFactory.getLog(OnlineMatrixFactorizationUDTF.class);
    private static final int RECORD_BYTES = (Integer.SIZE + Integer.SIZE + Integer.SIZE) / 8;

    // Option variables
    /** The number of latent factors */
    protected int factor;
    /** The regularization factors */
    protected float regU, regI, regJ;
    /** The regularization factor for Bias. reg / 2.0 by the default. */
    protected float regBias;
    /** Whether to use bias clause */
    protected boolean useBiasClause;
    /** The number of iterations */
    protected int iterations;

    protected LossFunction lossFunction;
    /** Initialization strategy of rank matrix */
    protected RankInitScheme rankInit;
    /** Learning rate */
    protected EtaEstimator etaEstimator;

    // Variable managing status of learning
    /** The number of processed training examples */
    protected long count;
    protected ConversionState cvState;

    // Model itself
    protected FactorizedModel model;

    // Input OIs and Context
    protected PrimitiveObjectInspector userOI;
    protected PrimitiveObjectInspector posItemOI;
    protected PrimitiveObjectInspector negItemOI;

    // Used for iterations
    protected NioFixedSegment fileIO;
    protected ByteBuffer inputBuf;
    private long lastWritePos;

    private float[] uProbe, iProbe, jProbe;

    public BPRMatrixFactorizationUDTF() {
        this.factor = 10;
        this.regU = 0.0025f;
        this.regI = 0.0025f;
        this.regJ = 0.00125f;
        this.regBias = 0.01f;
        this.useBiasClause = true;
        this.iterations = 30;
    }

    public enum LossFunction {
        sigmoid, logistic, lnLogistic;

        @Nonnull
        public static LossFunction resolve(@Nullable String name) {
            if (name == null) {
                return lnLogistic;
            }
            if (name.equalsIgnoreCase("lnLogistic")) {
                return lnLogistic;
            } else if (name.equalsIgnoreCase("logistic")) {
                return logistic;
            } else if (name.equalsIgnoreCase("sigmoid")) {
                return sigmoid;
            } else {
                throw new IllegalArgumentException("Unexpected loss function: " + name);
            }
        }
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("k", "factor", true, "The number of latent factor [default: 10]");
        opts.addOption("iter", "iterations", true, "The number of iterations [default: 30]");
        opts.addOption("loss", "loss_function", true,
            "Loss function [default: lnLogistic, logistic, sigmoid]");
        // initialization
        opts.addOption("rankinit", true,
            "Initialization strategy of rank matrix [random, gaussian] (default: random)");
        opts.addOption("maxval", "max_init_value", true,
            "The maximum initial value in the rank matrix [default: 1.0]");
        opts.addOption("min_init_stddev", true,
            "The minimum standard deviation of initial rank matrix [default: 0.1]");
        // regularization
        opts.addOption("reg", "lambda", true, "The regularization factor [default: 0.0025]");
        opts.addOption("reg_u", "reg_user", true,
            "The regularization factor for user [default: 0.0025 (reg)]");
        opts.addOption("reg_i", "reg_item", true,
            "The regularization factor for positive item [default: 0.0025 (reg)]");
        opts.addOption("reg_j", true,
            "The regularization factor for negative item [default: 0.00125 (reg_i/2) ]");
        // bias
        opts.addOption("reg_bias", true,
            "The regularization factor for bias clause [default: 0.01]");
        opts.addOption("disable_bias", "no_bias", false, "Turn off bias clause");
        // learning rates
        opts.addOption("eta", true, "The initial learning rate [default: 0.001]");
        opts.addOption("eta0", true, "The initial learning rate [default 0.3]");
        opts.addOption("t", "total_steps", true, "The total number of training examples");
        opts.addOption("power_t", true,
            "The exponent for inverse scaling learning rate [default 0.1]");
        opts.addOption("boldDriver", "bold_driver", false,
            "Whether to use Bold Driver for learning rate [default: false]");
        // conversion check
        opts.addOption("disable_cv", "disable_cvtest", false,
            "Whether to disable convergence check [default: enabled]");
        opts.addOption("cv_rate", "convergence_rate", true,
            "Threshold to determine convergence [default: 0.005]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;

        String lossFuncName = null;
        String rankInitOpt = null;
        float maxInitValue = 1.f;
        double initStdDev = 0.1d;
        boolean conversionCheck = true;
        double convergenceRate = 0.005d;

        if (argOIs.length >= 4) {
            String rawArgs = HiveUtils.getConstString(argOIs[3]);
            cl = parseOptions(rawArgs);

            this.factor = Primitives.parseInt(cl.getOptionValue("factor"), factor);
            this.iterations = Primitives.parseInt(cl.getOptionValue("iterations"), iterations);
            if (iterations < 1) {
                throw new UDFArgumentException(
                    "'-iterations' must be greater than or equals to 1: " + iterations);
            }
            lossFuncName = cl.getOptionValue("loss_function");

            float reg = Primitives.parseFloat(cl.getOptionValue("reg"), 0.0025f);
            this.regU = Primitives.parseFloat(cl.getOptionValue("reg_u"), reg);
            this.regI = Primitives.parseFloat(cl.getOptionValue("reg_i"), reg);
            this.regJ = Primitives.parseFloat(cl.getOptionValue("reg_j"), regI / 2.f);
            this.regBias = Primitives.parseFloat(cl.getOptionValue("reg_bias"), regBias);

            rankInitOpt = cl.getOptionValue("rankinit");
            maxInitValue = Primitives.parseFloat(cl.getOptionValue("max_init_value"), 1.f);
            initStdDev = Primitives.parseDouble(cl.getOptionValue("min_init_stddev"), 0.1d);

            conversionCheck = !cl.hasOption("disable_cvtest");
            convergenceRate = Primitives.parseDouble(cl.getOptionValue("cv_rate"), convergenceRate);
            this.useBiasClause = !cl.hasOption("no_bias");
        }

        this.lossFunction = LossFunction.resolve(lossFuncName);
        this.rankInit = RankInitScheme.resolve(rankInitOpt);
        rankInit.setMaxInitValue(maxInitValue);
        initStdDev = Math.max(initStdDev, 1.0d / factor);
        rankInit.setInitStdDev(initStdDev);
        this.etaEstimator = EtaEstimator.get(cl);
        this.cvState = new ConversionState(conversionCheck, convergenceRate);
        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 3 && argOIs.length != 4) {
            throw new UDFArgumentException(
                getClass().getSimpleName()
                        + " takes 3 or 4 arguments: INT user, INT posItem, INT negItem [, CONSTANT STRING options]");
        }
        this.userOI = HiveUtils.asIntCompatibleOI(argOIs[0]);
        this.posItemOI = HiveUtils.asIntCompatibleOI(argOIs[1]);
        this.negItemOI = HiveUtils.asIntCompatibleOI(argOIs[2]);

        processOptions(argOIs);

        this.model = new FactorizedModel(this, factor, rankInit);
        this.count = 0L;
        this.lastWritePos = 0L;
        this.uProbe = new float[factor];
        this.iProbe = new float[factor];
        this.jProbe = new float[factor];

        if (mapredContext != null && iterations > 1) {
            // invoke only at task node (initialize is also invoked in compilation)
            final File file;
            try {
                file = File.createTempFile("hivemall_bprmf", ".sgmt");
                file.deleteOnExit();
                if (!file.canWrite()) {
                    throw new UDFArgumentException("Cannot write a temporary file: "
                            + file.getAbsolutePath());
                }
            } catch (IOException ioe) {
                throw new UDFArgumentException(ioe);
            } catch (Throwable e) {
                throw new UDFArgumentException(e);
            }
            this.fileIO = new NioFixedSegment(file, RECORD_BYTES, false);
            this.inputBuf = ByteBuffer.allocateDirect(65536); // 64 KiB
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("idx");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("Pu");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));
        fieldNames.add("Qi");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));
        if (useBiasClause) {
            fieldNames.add("Bi");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        assert (args.length >= 3) : args.length;

        int u = PrimitiveObjectInspectorUtils.getInt(args[0], userOI);
        int i = PrimitiveObjectInspectorUtils.getInt(args[1], posItemOI);
        int j = PrimitiveObjectInspectorUtils.getInt(args[2], negItemOI);
        validateInput(u, i, j);

        beforeTrain(count, u, i, j);
        count++;
        train(u, i, j);
    }

    protected void beforeTrain(final long rowNum, final int u, final int i, final int j)
            throws HiveException {
        if (inputBuf != null) {
            assert (fileIO != null);
            final ByteBuffer buf = inputBuf;
            int remain = buf.remaining();
            if (remain < RECORD_BYTES) {
                writeBuffer(buf, fileIO, lastWritePos);
                this.lastWritePos = rowNum;
            }
            buf.putInt(u);
            buf.putInt(i);
            buf.putInt(j);
        }
    }

    protected void train(final int u, final int i, final int j) {
        Rating[] user = model.getUserVector(u, true);
        Rating[] itemI = model.getItemVector(i, true);
        Rating[] itemJ = model.getItemVector(j, true);

        copyToProbe(user, uProbe);
        copyToProbe(itemI, iProbe);
        copyToProbe(itemJ, jProbe);

        double x_uij = predict(u, i, uProbe, iProbe) - predict(u, j, uProbe, jProbe);

        final double dloss = dloss(x_uij, lossFunction);
        final float eta = eta();
        for (int k = 0, size = factor; k < size; k++) {
            float w_uf = uProbe[k];
            float h_if = iProbe[k];
            float h_jf = jProbe[k];

            updateUserRating(user[k], w_uf, h_if, h_jf, dloss, eta);
            updateItemRating(itemI[k], w_uf, h_if, dloss, eta, regI); // positive item
            updateItemRating(itemJ[k], w_uf, h_jf, -dloss, eta, regJ); // negative item
        }
        if (useBiasClause) {
            updateBias(i, j, dloss, eta);
        }
    }

    protected double predict(final int user, final int item, @Nonnull final float[] userProbe,
            @Nonnull final float[] itemProbe) {
        double ret = model.getItemBias(item);
        for (int k = 0, size = factor; k < size; k++) {
            ret += userProbe[k] * itemProbe[k];
        }
        if (!NumberUtils.isFinite(ret)) {
            throw new IllegalStateException("Detected " + ret + " in predict where user=" + user
                    + " and item=" + item);
        }
        return ret;
    }

    protected double dloss(final double x, @Nonnull final LossFunction loss) {
        switch (loss) {
            case sigmoid: {
                return 1.d / (1.d + Math.exp(x));
            }
            case logistic: {
                double sigmoid = MathUtils.sigmoid(x);
                return sigmoid * (1.d - sigmoid);
            }
            case lnLogistic: {
                double ex = Math.exp(-x);
                return ex / (1.d + ex);
            }
            default: {
                throw new IllegalStateException("Unexpectd loss function: " + loss);
            }
        }
    }

    protected float eta() {
        return etaEstimator.eta(count);
    }

    protected void updateUserRating(final Rating rating, final float w_uf, final float h_if,
            final float h_jf, final double dloss, final float eta) {
        double grad = dloss * (h_if - h_jf) - regU * w_uf;
        float delta = (float) (eta * grad);
        float newWeight = w_uf + delta;
        if (!NumberUtils.isFinite(newWeight)) {
            throw new IllegalStateException("Detected " + newWeight + " for w_uf");
        }
        rating.setWeight(newWeight);
        cvState.incrLoss(regU * w_uf * w_uf);
    }

    protected void updateItemRating(final Rating rating, final float w_uf, final float h_f,
            final double dloss, final float eta, final float reg) {
        double grad = dloss * w_uf - reg * h_f;
        float delta = (float) (eta * grad);
        float newWeight = h_f + delta;
        if (!NumberUtils.isFinite(newWeight)) {
            throw new IllegalStateException("Detected " + newWeight + " for h_f");
        }
        rating.setWeight(newWeight);
        cvState.incrLoss(reg * h_f * h_f);
    }

    protected void updateBias(final int i, final int j, final double dloss, final float eta) {
        float Bi = model.getItemBias(i);
        double Gi = dloss - regBias * Bi;
        Bi += eta * Gi;
        if (!NumberUtils.isFinite(Bi)) {
            throw new IllegalStateException("Detected " + Bi + " for Bi");
        }
        model.setItemBias(i, Bi);
        cvState.incrLoss(regBias * Bi * Bi);

        float Bj = model.getItemBias(j);
        double Gj = -dloss - regBias * Bj;
        Bj += eta * Gj;
        if (!NumberUtils.isFinite(Bj)) {
            throw new IllegalStateException("Detected " + Bj + " for Bj");
        }
        model.setItemBias(j, Bj);
        cvState.incrLoss(regBias * Bj * Bj);
    }

    @Override
    public void close() throws HiveException {
        if (model != null) {
            if (count == 0) {
                this.model = null; // help GC
                return;
            }
            if (iterations > 1) {
                runIterativeTraining(iterations);
            }

            final IntWritable idx = new IntWritable();
            final FloatWritable[] Pu = HiveUtils.newFloatArray(factor, 0.f);
            final FloatWritable[] Qi = HiveUtils.newFloatArray(factor, 0.f);
            final FloatWritable Bi = useBiasClause ? new FloatWritable() : null;
            final Object[] forwardObj = new Object[] {idx, Pu, Qi, Bi};

            int numForwarded = 0;
            for (int i = model.getMinIndex(), maxIdx = model.getMaxIndex(); i <= maxIdx; i++) {
                idx.set(i);
                Rating[] userRatings = model.getUserVector(i);
                if (userRatings == null) {
                    forwardObj[1] = null;
                } else {
                    forwardObj[1] = Pu;
                    copyTo(userRatings, Pu);
                }
                Rating[] itemRatings = model.getItemVector(i);
                if (itemRatings == null) {
                    forwardObj[2] = null;
                } else {
                    forwardObj[2] = Qi;
                    copyTo(itemRatings, Qi);
                }
                if (useBiasClause) {
                    Bi.set(model.getItemBias(i));
                }
                forward(forwardObj);
                numForwarded++;
            }
            this.model = null; // help GC
            LOG.info("Forwarded the prediction model of " + numForwarded + " rows. [lastLosses="
                    + cvState.getCumulativeLoss() + ", #trainingExamples=" + count + "]");
        }
    }

    private final void runIterativeTraining(@Nonnegative final int iterations) throws HiveException {
        final ByteBuffer inputBuf = this.inputBuf;
        final NioFixedSegment fileIO = this.fileIO;
        assert (inputBuf != null);
        assert (fileIO != null);
        final long numTrainingExamples = count;

        final Reporter reporter = getReporter();
        final Counter iterCounter = (reporter == null) ? null : reporter.getCounter(
            "hivemall.mf.BPRMatrixFactorization$Counter", "iteration");

        try {
            if (lastWritePos == 0) {// run iterations w/o temporary file
                if (inputBuf.position() == 0) {
                    return; // no training example
                }
                inputBuf.flip();

                int iter = 2;
                for (; iter <= iterations; iter++) {
                    reportProgress(reporter);
                    setCounterValue(iterCounter, iter);

                    while (inputBuf.remaining() > 0) {
                        int u = inputBuf.getInt();
                        int i = inputBuf.getInt();
                        int j = inputBuf.getInt();
                        // invoke train
                        count++;
                        train(u, i, j);
                    }
                    cvState.multiplyLoss(0.5d);
                    cvState.logState(iter, eta());
                    if (cvState.isConverged(iter, numTrainingExamples)) {
                        break;
                    }
                    if (cvState.isLossIncreased()) {
                        etaEstimator.update(1.1f);
                    } else {
                        etaEstimator.update(0.5f);
                    }
                    inputBuf.rewind();
                }
                LOG.info("Performed " + Math.max(iter, iterations) + " iterations of "
                        + NumberUtils.formatNumber(numTrainingExamples)
                        + " training examples on memory (thus " + NumberUtils.formatNumber(count)
                        + " training updates in total) ");
            } else {// read training examples in the temporary file and invoke train for each example

                // write training examples in buffer to a temporary file
                if (inputBuf.position() > 0) {
                    writeBuffer(inputBuf, fileIO, lastWritePos);
                } else if (lastWritePos == 0) {
                    return; // no training example
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
                    long seekPos = 0L;
                    while (true) {
                        reportProgress(reporter);
                        // TODO prefetch
                        // writes training examples to a buffer in the temporary file
                        final int bytesRead;
                        try {
                            bytesRead = fileIO.read(seekPos, inputBuf);
                        } catch (IOException e) {
                            throw new HiveException("Failed to read a file: "
                                    + fileIO.getFile().getAbsolutePath(), e);
                        }
                        if (bytesRead == 0) { // reached file EOF
                            break;
                        }
                        assert (bytesRead > 0) : bytesRead;
                        seekPos += bytesRead;

                        // reads training examples from a buffer
                        inputBuf.flip();
                        int remain = inputBuf.remaining();
                        assert (remain > 0) : remain;
                        for (; remain >= RECORD_BYTES; remain -= RECORD_BYTES) {
                            int u = inputBuf.getInt();
                            int i = inputBuf.getInt();
                            int j = inputBuf.getInt();
                            // invoke train
                            count++;
                            train(u, i, j);
                        }
                        inputBuf.compact();
                    }
                    cvState.multiplyLoss(0.5d);
                    cvState.logState(iter, eta());
                    if (cvState.isConverged(iter, numTrainingExamples)) {
                        break;
                    }
                    if (cvState.isLossIncreased()) {
                        etaEstimator.update(1.1f);
                    } else {
                        etaEstimator.update(0.5f);
                    }
                }
                LOG.info("Performed " + Math.max(iter, iterations) + " iterations of "
                        + NumberUtils.formatNumber(numTrainingExamples)
                        + " training examples using a secondary storage (thus "
                        + NumberUtils.formatNumber(count) + " training updates in total)");
            }
        } finally {
            // delete the temporary file and release resources
            try {
                fileIO.close(true);
            } catch (IOException e) {
                throw new HiveException("Failed to close a file: "
                        + fileIO.getFile().getAbsolutePath(), e);
            }
            this.inputBuf = null;
            this.fileIO = null;
        }
    }

    @Override
    public Rating newRating(float v) {
        return new Rating(v);
    }

    // ----------------------------------------------
    // static utility methods

    private static void validateInput(int u, int i, int j) throws HiveException {
        if (u < 0) {
            throw new HiveException("Illegal u index: " + u);
        }
        if (i < 0) {
            throw new HiveException("Illegal i index: " + i);
        }
        if (j < 0) {
            throw new HiveException("Illegal j index: " + j);
        }
    }

    private static void writeBuffer(@Nonnull final ByteBuffer srcBuf,
            @Nonnull final NioFixedSegment dst, final long lastWritePos) throws HiveException {
        // TODO asynchronous write in the background
        srcBuf.flip();
        try {
            dst.writeRecords(lastWritePos, srcBuf);
        } catch (IOException e) {
            throw new HiveException("Exception causes while writing records to : " + lastWritePos,
                e);
        }
        srcBuf.clear();
    }

    @Nonnull
    private final void copyToProbe(@Nonnull final Rating[] rating, @Nonnull float[] probe) {
        for (int k = 0, size = factor; k < size; k++) {
            probe[k] = rating[k].getWeight();
        }
    }

    private static void copyTo(@Nonnull final Rating[] rating, @Nonnull final FloatWritable[] dst) {
        for (int k = 0, size = rating.length; k < size; k++) {
            float w = rating[k].getWeight();
            dst[k].set(w);
        }
    }
}
