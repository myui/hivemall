/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.mf;

import hivemall.UDTFWithOptions;
import hivemall.common.RatingInitilizer;
import hivemall.io.FactorizedModel;
import hivemall.io.FactorizedModel.RankInitScheme;
import hivemall.io.Rating;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.FileUtils;
import hivemall.utils.io.NioFixedSegment;
import hivemall.utils.io.Segments;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Primitives;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public abstract class OnlineMatrixFactorizationUDTF extends UDTFWithOptions
        implements RatingInitilizer {
    private static final Log logger = LogFactory.getLog(OnlineMatrixFactorizationUDTF.class);
    private static final int RECORD_BYTES = (Integer.SIZE + Integer.SIZE + Double.SIZE) / 8;

    // Option variables
    /** The number of latent factors */
    protected int factor;
    /** The regularization factor */
    protected float lambda;
    /** The initial mean rating */
    protected float meanRating;
    /** Whether update (and return) the mean rating or not */
    protected boolean updateMeanRating;
    /** The number of iterations */
    protected int iterations;
    /** Whether to check conversion */
    protected boolean conversionCheck;
    /** Threshold to determine convergence */
    protected double convergenceRate;
    /** being ready to end iteration */
    protected boolean readyToFinishIterations;

    /** Initialization strategy of rank matrix */
    protected RankInitScheme rankInit;

    // Model itself
    protected FactorizedModel model;

    // Variable managing status of learning
    /** The number of processed training examples */
    protected long count;
    /** The cumulative errors in the training */
    protected double totalErrors;
    /** The cumulative losses in an iteration */
    protected double currLosses, prevLosses;

    // Input OIs and Context
    protected IntObjectInspector userOI;
    protected IntObjectInspector itemOI;
    protected PrimitiveObjectInspector ratingOI;
    protected MapredContext mapredContext;

    // Used for iterations
    protected Segments fileIO;
    protected ByteBuffer inputBuf;
    private long lastWritePos;

    private float[] userProbe, itemProbe;

    public OnlineMatrixFactorizationUDTF() {
        this.mapredContext = null;
        this.factor = 10;
        this.lambda = 0.03f;
        this.meanRating = 0.f;
        this.updateMeanRating = false;
        this.iterations = 1;
        this.conversionCheck = true;
        this.convergenceRate = 0.005d;
        this.readyToFinishIterations = false;
    }

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("k", "factor", true, "The number of latent factor [default: 10]");
        opts.addOption("r", "lambda", true, "The regularization factor [default: 0.03]");
        opts.addOption("mu", "mean_rating", true, "The mean rating [default: 0.0]");
        opts.addOption("update_mean", "update_mu", false, "Whether update (and return) the mean rating or not");
        opts.addOption("rankinit", true, "Initialization strategy of rank matrix [default: guassian, random]");
        opts.addOption("maxval", "max_init_value", true, "The maximum initial value in the rank matrix [default: 1.0]");
        opts.addOption("min_init_stddev", true, "The minimum standard deviation of initial rank matrix [default: 0.1]");
        opts.addOption("iter", "iterations", true, "The number of iterations [default: 1]");
        opts.addOption("disable_cv", "disable_cvtest", false, "Whether to disable convergence check [default: enabled]");
        opts.addOption("cv_rate", "convergence_rate", true, "Threshold to determine convergence [default: 0.005]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        CommandLine cl = null;
        String rankInitOpt = null;
        float maxInitValue = 1.f;
        double initStdDev = 0.1d;
        if(argOIs.length >= 4) {
            String rawArgs = HiveUtils.getConstString(argOIs[3]);
            cl = parseOptions(rawArgs);
            this.factor = Primitives.parseInt(cl.getOptionValue("factor"), 10);
            this.lambda = Primitives.parseFloat(cl.getOptionValue("lambda"), 0.03f);
            this.meanRating = Primitives.parseFloat(cl.getOptionValue("mu"), 0.f);
            this.updateMeanRating = cl.hasOption("update_mean");
            rankInitOpt = cl.getOptionValue("rankinit");
            maxInitValue = Primitives.parseFloat(cl.getOptionValue("max_init_value"), 1.f);
            initStdDev = Primitives.parseDouble(cl.getOptionValue("min_init_stddev"), 0.1d);
            this.iterations = Primitives.parseInt(cl.getOptionValue("iterations"), 1);
            if(iterations < 1) {
                throw new UDFArgumentException("'-iterations' must be greater than or equals to 1: "
                        + iterations);
            }
            this.conversionCheck = !cl.hasOption("disable_cvtest");
            this.convergenceRate = Primitives.parseDouble(cl.getOptionValue("cv_rate"), 0.005d);
        }
        this.rankInit = RankInitScheme.resolve(rankInitOpt);
        rankInit.setMaxInitValue(maxInitValue);
        initStdDev = Math.max(initStdDev, 1.0d / factor);
        rankInit.setInitStdDev(initStdDev);
        return cl;
    }

    @Override
    public final void configure(MapredContext mapredContext) {
        this.mapredContext = mapredContext;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length < 3) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 3 arguments: INT user, INT item, FLOAT rating [, CONSTANT STRING options]");
        }
        this.userOI = HiveUtils.asIntOI(argOIs[0]);
        this.itemOI = HiveUtils.asIntOI(argOIs[1]);
        this.ratingOI = HiveUtils.asDoubleCompatibleOI(argOIs[2]);

        processOptions(argOIs);

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("idx");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("Pu");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));
        fieldNames.add("Qi");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));
        fieldNames.add("Bu");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        fieldNames.add("Bi");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        if(updateMeanRating) {
            fieldNames.add("mu");
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        }

        this.model = new FactorizedModel(this, factor, meanRating, rankInit);
        this.count = 0L;
        this.totalErrors = 0.d;
        this.currLosses = 0.d;
        this.prevLosses = Double.POSITIVE_INFINITY;
        this.lastWritePos = 0L;
        this.userProbe = new float[factor];
        this.itemProbe = new float[factor];

        if(mapredContext != null && iterations > 1) {
            // invoke only at task node (initialize is also invoked in compilation)
            final File file;
            try {
                file = File.createTempFile("hivemall_mf", ".sgmt");
                file.deleteOnExit();
                if(!file.canWrite()) {
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

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Rating newRating(float v) {
        return new Rating(v);
    }

    @Override
    public final void process(Object[] args) throws HiveException {
        assert (args.length >= 3) : args.length;

        int user = userOI.get(args[0]);
        if(user < 0) {
            throw new HiveException("Illegal user index: " + user);
        }
        int item = itemOI.get(args[1]);
        if(item < 0) {
            throw new HiveException("Illegal item index: " + user);
        }
        double rating = PrimitiveObjectInspectorUtils.getDouble(args[2], ratingOI);

        beforeTrain(count, user, item, rating);
        count++;
        train(user, item, rating);
    }

    @Nonnull
    protected final float[] copyToUserProbe(@Nonnull final Rating[] rating) {
        for(int k = 0, size = factor; k < size; k++) {
            userProbe[k] = rating[k].getWeight();
        }
        return userProbe;
    }

    @Nonnull
    protected final float[] copyToItemProbe(@Nonnull final Rating[] rating) {
        for(int k = 0, size = factor; k < size; k++) {
            itemProbe[k] = rating[k].getWeight();
        }
        return itemProbe;
    }

    protected void train(final int user, final int item, final double rating) throws HiveException {
        final Rating[] users = model.getUserVector(user, true);
        assert (users != null);
        final Rating[] items = model.getItemVector(item, true);
        assert (items != null);
        final float[] userProbe = copyToUserProbe(users);
        final float[] itemProbe = copyToItemProbe(items);

        final double err = rating - predict(user, item, userProbe, itemProbe);
        this.totalErrors += Math.abs(err);
        this.currLosses += err * err;

        final float eta = eta();
        for(int k = 0, size = factor; k < size; k++) {
            float Pu = userProbe[k];
            float Qi = itemProbe[k];
            updateItemRating(items[k], Pu, Qi, err, eta);
            updateUserRating(users[k], Pu, Qi, err, eta);
        }
        updateBias(user, item, err, eta);
        if(updateMeanRating) {
            updateMeanRating(err, eta);
        }

        onUpdate(user, item, users, items, err);
    }

    protected void beforeTrain(final long rowNum, final int user, final int item, final double rating)
            throws HiveException {
        if(inputBuf != null) {
            assert (fileIO != null);
            final ByteBuffer buf = inputBuf;
            int remain = buf.remaining();
            if(remain < RECORD_BYTES) {
                writeBuffer(buf, fileIO, lastWritePos);
                this.lastWritePos = rowNum;
            }
            buf.putInt(user);
            buf.putInt(item);
            buf.putDouble(rating);
        }
    }

    protected void onUpdate(final int user, final int item, final Rating[] users, final Rating[] items, final double err)
            throws HiveException {}

    protected double predict(final int user, final int item, final float[] userProbe, final float[] itemProbe) {
        double ret = bias(user, item);
        for(int k = 0, size = factor; k < size; k++) {
            ret += userProbe[k] * itemProbe[k];
        }
        return ret;
    }

    protected double predict(final int user, final int item) throws HiveException {
        final Rating[] users = model.getUserVector(user);
        if(users == null) {
            throw new HiveException("User rating is not found: " + user);
        }
        final Rating[] items = model.getItemVector(item);
        if(items == null) {
            throw new HiveException("Item rating is not found: " + item);
        }
        double ret = bias(user, item);
        for(int k = 0, size = factor; k < size; k++) {
            ret += users[k].getWeight() * items[k].getWeight();
        }
        return ret;
    }

    protected double bias(final int user, final int item) {
        return model.getMeanRating() + model.getUserBias(user) + model.getItemBias(item);
    }

    protected float eta() {
        return 1.f; // dummy
    }

    protected void updateItemRating(final Rating rating, final float Pu, final float Qi, final double err, final float eta) {
        double grad = err * Pu - lambda * Qi;
        float newQi = Qi + (float) (eta * grad);
        rating.setWeight(newQi);
        this.currLosses += lambda * Qi * Qi;
    }

    protected void updateUserRating(final Rating rating, final float Pu, final float Qi, final double err, final float eta) {
        double grad = err * Qi - lambda * Pu;
        float newPu = Pu + (float) (eta * grad);
        rating.setWeight(newPu);
        this.currLosses += lambda * Pu * Pu;
    }

    protected void updateMeanRating(final double err, final float eta) {
        assert updateMeanRating;
        float mean = model.getMeanRating();
        mean += eta * err;
        model.setMeanRating(mean);
    }

    protected void updateBias(final int user, final int item, final double err, final float eta) {
        float Bu = model.getUserBias(user);
        double Gu = err - lambda * Bu;
        Bu += eta * Gu;
        model.setUserBias(user, Bu);
        this.currLosses += lambda * Bu * Bu;

        float Bi = model.getItemBias(item);
        double Gi = err - lambda * Bi;
        Bi += eta * Gi;
        model.setItemBias(item, Bi);
        this.currLosses += lambda * Bi * Bi;
    }

    @Override
    public void close() throws HiveException {
        if(model != null) {
            if(count == 0) {
                this.model = null; // help GC
                return;
            }
            if(iterations > 1) {
                runIterativeTraining(iterations);
            }
            final IntWritable idx = new IntWritable();
            final FloatWritable[] Pu = HiveUtils.newFloatArray(factor, 0.f);
            final FloatWritable[] Qi = HiveUtils.newFloatArray(factor, 0.f);
            final FloatWritable Bu = new FloatWritable();
            final FloatWritable Bi = new FloatWritable();
            final Object[] forwardObj;
            if(updateMeanRating) {
                float meanRating = model.getMeanRating();
                FloatWritable mu = new FloatWritable(meanRating);
                forwardObj = new Object[] { idx, Pu, Qi, Bu, Bi, mu };
            } else {
                forwardObj = new Object[] { idx, Pu, Qi, Bu, Bi };
            }
            int numForwarded = 0;
            for(int i = model.getMinIndex(), maxIdx = model.getMaxIndex(); i <= maxIdx; i++) {
                idx.set(i);
                Rating[] userRatings = model.getUserVector(i);
                if(userRatings == null) {
                    forwardObj[1] = null;
                } else {
                    forwardObj[1] = Pu;
                    copyTo(userRatings, Pu);
                }
                Rating[] itemRatings = model.getItemVector(i);
                if(itemRatings == null) {
                    forwardObj[2] = null;
                } else {
                    forwardObj[2] = Qi;
                    copyTo(itemRatings, Qi);
                }
                Bu.set(model.getUserBias(i));
                Bi.set(model.getItemBias(i));
                forward(forwardObj);
                numForwarded++;
            }
            this.model = null; // help GC
            logger.info("Forwarded the prediction model of " + numForwarded
                    + " rows. [totalErrors=" + totalErrors + ", lastLosses=" + currLosses
                    + ", #trainingExamples=" + count + "]");
        }
    }

    protected static void writeBuffer(@Nonnull final ByteBuffer srcBuf, @Nonnull final Segments dst, final long lastWritePos)
            throws HiveException {
        // TODO asynchronous write in the background
        srcBuf.flip();
        try {
            dst.write(lastWritePos, srcBuf);
        } catch (IOException e) {
            throw new HiveException("Exception causes while writing records to : " + lastWritePos, e);
        }
        srcBuf.clear();
    }

    protected final void runIterativeTraining(@Nonnegative final int iterations)
            throws HiveException {
        final ByteBuffer inputBuf = this.inputBuf;
        final Segments fileIO = this.fileIO;
        assert (inputBuf != null);
        assert (fileIO != null);

        try {
            if(lastWritePos == 0) {// run iterations w/o temporary file
                if(inputBuf.position() == 0) {
                    return; // no training example
                }
                inputBuf.flip();
                for(int i = 1; i < iterations; i++) {
                    while(inputBuf.remaining() > 0) {
                        int user = inputBuf.getInt();
                        int item = inputBuf.getInt();
                        double rating = inputBuf.getDouble();
                        // invoke train
                        count++;
                        train(user, item, rating);
                    }
                    inputBuf.rewind();
                }
            } else {// read training examples in the temporary file and invoke train for each example

                // write training examples in buffer to a temporary file
                if(inputBuf.position() > 0) {
                    writeBuffer(inputBuf, fileIO, lastWritePos);
                } else if(lastWritePos == 0) {
                    return; // no training example
                }
                try {
                    fileIO.flush();
                } catch (IOException e) {
                    throw new HiveException("Failed to flush a file: "
                            + fileIO.getFile().getAbsolutePath(), e);
                }
                final long numTrainingExamples = count;
                if(logger.isInfoEnabled()) {
                    File tmpFile = fileIO.getFile();
                    logger.info("Wrote " + numTrainingExamples
                            + " records to a temporary file for iterative training: "
                            + tmpFile.getAbsolutePath() + " (" + FileUtils.prettyFileSize(tmpFile)
                            + ")");
                }

                // run iterations
                int i = 1;
                for(; i < iterations; i++) {
                    this.currLosses /= 2.0d;
                    if(conversionCheck && isConverged(i + 1)) {
                        break;
                    }

                    inputBuf.clear();
                    long seekPos = 0L;
                    while(true) {
                        // TODO prefetch
                        // writes training examples to a buffer in the temporary file
                        final int bytesRead;
                        try {
                            bytesRead = fileIO.directRead(seekPos, inputBuf);
                        } catch (IOException e) {
                            throw new HiveException("Failed to read a file: "
                                    + fileIO.getFile().getAbsolutePath(), e);
                        }
                        if(bytesRead == 0) { // reached file EOF
                            break;
                        }
                        assert (bytesRead > 0) : bytesRead;
                        seekPos += bytesRead;

                        // reads training examples from a buffer
                        inputBuf.flip();
                        int remain = inputBuf.remaining();
                        assert (remain > 0) : remain;
                        for(; remain >= RECORD_BYTES; remain -= RECORD_BYTES) {
                            int user = inputBuf.getInt();
                            int item = inputBuf.getInt();
                            double rating = inputBuf.getDouble();
                            // invoke train
                            count++;
                            train(user, item, rating);
                        }
                        inputBuf.compact();
                    }
                }
                logger.info("Performed " + i + " iterations of "
                        + NumberUtils.formatNumber(numTrainingExamples)
                        + " training examples (thus " + NumberUtils.formatNumber(count)
                        + " training updates in total)");
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

    private boolean isConverged(final int iter) {
        assert conversionCheck;
        if(currLosses > prevLosses) {
            if(logger.isInfoEnabled()) {
                logger.info("currLoss [" + currLosses + "] > prevLosses [" + prevLosses + "]");
            }
            this.prevLosses = currLosses;
            this.currLosses = 0.d;
            this.readyToFinishIterations = false;
            return false;
        }

        final double changeRate = (prevLosses - currLosses) / prevLosses;
        if(changeRate < convergenceRate) {
            if(readyToFinishIterations) {
                // NOTE: never be true at the first iteration where prevLosses == Double.POSITIVE_INFINITY
                logger.info("Training converged at " + iter + "-th iteration. [curLosses="
                        + currLosses + ", prevLosses=" + prevLosses + ", changeRate=" + changeRate
                        + "]");
                return true;
            } else {
                this.readyToFinishIterations = true;
            }
        } else {
            if(logger.isDebugEnabled()) {
                logger.debug("iter: " + iter + "[curLosses=" + currLosses + ", prevLosses="
                        + prevLosses + ", changeRate=" + changeRate + ", #trainingExamples="
                        + count + "]");
            }
            this.readyToFinishIterations = false;
        }

        this.prevLosses = currLosses;
        this.currLosses = 0.d;
        return false;
    }

    private static void copyTo(@Nonnull final Rating[] rating, @Nonnull final FloatWritable[] dst) {
        for(int k = 0, size = rating.length; k < size; k++) {
            float w = rating[k].getWeight();
            dst[k].set(w);
        }
    }

}
