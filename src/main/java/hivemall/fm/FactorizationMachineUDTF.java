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
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.FileUtils;
import hivemall.utils.io.NioStatefullSegment;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Primitives;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public final class FactorizationMachineUDTF extends UDTFWithOptions {
    private static final Log logger = LogFactory.getLog(FactorizationMachineUDTF.class);
    private static final int INT_BYTES = Integer.SIZE / 8;

    private ListObjectInspector _xOI;
    private PrimitiveObjectInspector _yOI;

    // Learning hyper-parameters/options    
    private boolean _classification;
    private long _seed;
    private int _iterations;
    private int _factor;
    private float _lambda0;
    private double _sigma;

    // Hyperparameter for regression
    private double _min_target;
    private double _max_target;

    private EtaEstimator _etaEstimator;
    /**
     * The size of x
     */
    private int _p;

    private FactorizationMachineModel _model;

    /**
     * The number of training examples processed
     */
    private long _t;
    private ConversionState _cvState;

    // file IO
    private ByteBuffer _inputBuf;
    private NioStatefullSegment _fileIO;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("c", "classification", false, "Act as classification");
        opts.addOption("seed", true, "Seed value [default: -1 (random)]");
        opts.addOption("iters", "iterations", true, "The number of iterations [default: 1]");
        opts.addOption("p", "size_x", true, "The size of x");
        opts.addOption("f", "factor", true, "The number of the latent variables [default: 10]");
        opts.addOption("sigma", true, "The standard deviation for initializing V [default: 0.1]");
        opts.addOption("lambda", "lambda0", true, "The initial lambda value for regularization [default: 0.01]");
        // regression
        opts.addOption("min_target", true, "The minimum value of target variable");
        opts.addOption("max_target", true, "The maximum value of target variable");
        // learning rates
        opts.addOption("eta", true, "The initial learning rate");
        opts.addOption("eta0", true, "The initial learning rate [default 0.1]");
        opts.addOption("t", "total_steps", true, "The total number of training examples");
        opts.addOption("power_t", true, "The exponent for inverse scaling learning rate [default 0.1]");
        // conversion check
        opts.addOption("disable_cv", "disable_cvtest", false, "Whether to disable convergence check [default: enabled]");
        opts.addOption("cv_rate", "convergence_rate", true, "Threshold to determine convergence [default: 0.005]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        boolean classication = false;
        long seed = -1L;
        int iters = 1;
        int p = -1;
        int factor = 10;
        float lambda0 = 0.01f;
        double sigma = 0.1d;
        double min_target = Double.MAX_VALUE, max_target = Double.MIN_VALUE;
        boolean conversionCheck = true;
        double convergenceRate = 0.005d;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);
            classication = cl.hasOption("classification");
            seed = Primitives.parseLong(cl.getOptionValue("seed"), seed);
            iters = Primitives.parseInt(cl.getOptionValue("iterations"), iters);
            p = Primitives.parseInt(cl.getOptionValue("size_x"), p);
            factor = Primitives.parseInt(cl.getOptionValue("factor"), factor);
            lambda0 = Primitives.parseFloat(cl.getOptionValue("lambda0"), lambda0);
            sigma = Primitives.parseDouble(cl.getOptionValue("sigma"), sigma);
            min_target = Primitives.parseDouble(cl.getOptionValue("min_target"), _min_target);
            max_target = Primitives.parseDouble(cl.getOptionValue("max_target"), _max_target);
            conversionCheck = !cl.hasOption("disable_cvtest");
            convergenceRate = Primitives.parseDouble(cl.getOptionValue("cv_rate"), convergenceRate);
        }

        this._classification = classication;
        this._seed = (seed == -1L) ? System.nanoTime() : seed;
        this._iterations = iters;
        this._p = p;
        this._factor = factor;
        this._lambda0 = lambda0;
        this._sigma = sigma;
        this._min_target = min_target;
        this._max_target = max_target;
        this._etaEstimator = EtaEstimator.get(cl);
        this._cvState = new ConversionState(conversionCheck, convergenceRate);

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 2 or 3 arguments: array<string> x, double y [, CONSTANT STRING options]: "
                    + Arrays.toString(argOIs));
        }

        this._xOI = HiveUtils.asListOI(argOIs[0]);
        if(!HiveUtils.isStringOI(_xOI.getListElementObjectInspector())) {
            throw new UDFArgumentException("Unexpected Object inspector for array<string>: "
                    + argOIs[0]);
        }
        this._yOI = HiveUtils.asDoubleCompatibleOI(argOIs[1]);

        if(_p == -1) {
            this._model = new FMMapModel(_classification, _factor, _lambda0, _sigma, _seed, _min_target, _max_target, _etaEstimator);
        } else {
            this._model = new FMArrayModel(_classification, _factor, _lambda0, _sigma, _p, _seed, _min_target, _max_target, _etaEstimator);
        }
        this._t = 0L;

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("idx");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("W_i");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        fieldNames.add("V_if");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector));

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Feature[] x = parseFeatures(args[0], _xOI);
        if(x == null) {
            return;
        }
        double y = PrimitiveObjectInspectorUtils.getDouble(args[1], _yOI);
        if(_classification) {
            y = (y > 0.d) ? 1.d : -1.d;
        }

        ++_t;
        recordTrain(x, y);
        train(x, y);
    }

    protected void recordTrain(@Nonnull final Feature[] x, final double y) throws HiveException {
        if(_iterations <= 1) {
            return;
        }

        ByteBuffer inputBuf = _inputBuf;
        NioStatefullSegment dst = _fileIO;
        if(inputBuf == null) {
            final File file;
            try {
                file = File.createTempFile("hivemall_fm", ".sgmt");
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

            this._inputBuf = inputBuf = ByteBuffer.allocateDirect(65536); // 64 KiB
            this._fileIO = dst = new NioStatefullSegment(file, false);
        }

        int xBytes = requiredBytes(x);
        int recordBytes = (Integer.SIZE + Double.SIZE) / 8 + xBytes;
        int requiredBytes = (Integer.SIZE / 8) + recordBytes;
        int remain = inputBuf.remaining();
        if(remain < requiredBytes) {
            writeBuffer(inputBuf, dst);
        }

        inputBuf.putInt(recordBytes);
        inputBuf.putInt(x.length);
        for(Feature f : x) {
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

    public void train(@Nonnull final Feature[] x, final double y) throws HiveException {
        // check
        _model.check(x);

        final float eta = _etaEstimator.eta(_t);
        final double dloss = _model.dloss(x, y);

        // w0 update
        _model.updateW0(x, dloss, eta);

        for(Feature e : x) {
            if(e == null) {
                continue;
            }
            int i = e.index;
            double xi = e.value;
            // wi update
            _model.updateWi(x, dloss, i, xi, eta);
            for(int f = 0, k = _factor; f < k; f++) {
                // Vif update
                _model.updateV(x, dloss, i, f, eta);
            }
        }
    }

    @Override
    public void close() throws HiveException {
        if(_t == 0) {
            this._model = null;
            return;
        }
        if(_iterations > 1) {
            runTrainingIteration(_iterations);
        }

        final int P = _model.getSize();
        if(P <= 0) {
            throw new HiveException("Invalid P SIZE:" + P);
        }

        final IntWritable idx = new IntWritable(0);
        final FloatWritable Wi = new FloatWritable(0.f);
        final FloatWritable[] Vi = HiveUtils.newFloatArray(_factor, 0.f);

        final Object[] forwardObjs = new Object[3];
        forwardObjs[0] = idx;
        forwardObjs[1] = Wi;
        forwardObjs[2] = null;
        // W0
        idx.set(0);
        // V0
        Wi.set(_model.getW(0));
        forward(forwardObjs);

        // Wi, Vif (i starts from 1..P)
        forwardObjs[2] = Arrays.asList(Vi);
        for(int i = 1; i <= P; i++) {
            idx.set(i);
            // set Wi
            float w = _model.getW(i);
            Wi.set(w);
            // set Vif
            for(int f = 0; f < _factor; f++) {
                float v = _model.getV(i, f);
                Vi[f].set(v);
            }
            forward(forwardObjs);
        }
    }

    protected void runTrainingIteration(int iterations) throws HiveException {
        final ByteBuffer inputBuf = this._inputBuf;
        final NioStatefullSegment fileIO = this._fileIO;
        assert (inputBuf != null);
        assert (fileIO != null);

        try {
            if(fileIO.getPosition() == 0L) {// run iterations w/o temporary file
                if(inputBuf.position() == 0) {
                    return; // no training example
                }
                inputBuf.flip();
                for(int i = 1; i < iterations; i++) {
                    while(inputBuf.remaining() > 0) {
                        int bytes = inputBuf.getInt();
                        assert (bytes > 0) : bytes;
                        int xLength = inputBuf.getInt();
                        final Feature[] x = new Feature[xLength];
                        for(int j = 0; j < xLength; j++) {
                            x[j] = new Feature(inputBuf);
                        }
                        double y = inputBuf.getDouble();
                        // invoke train
                        ++_t;
                        train(x, y);
                    }
                    inputBuf.rewind();
                }
            } else {// read training examples in the temporary file and invoke train for each example

                // write training examples in buffer to a temporary file
                if(inputBuf.remaining() > 0) {
                    writeBuffer(inputBuf, fileIO);
                }
                try {
                    fileIO.flush();
                } catch (IOException e) {
                    throw new HiveException("Failed to flush a file: "
                            + fileIO.getFile().getAbsolutePath(), e);
                }
                final long numTrainingExamples = _t;
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
                    _cvState.multiplyLoss(0.5d);
                    if(_cvState.isConverged(i + 1, numTrainingExamples)) {
                        break;
                    }

                    inputBuf.clear();
                    fileIO.resetPosition();
                    while(true) {
                        // TODO prefetch
                        // writes training examples to a buffer in the temporary file
                        final int bytesRead;
                        try {
                            bytesRead = fileIO.read(inputBuf);
                        } catch (IOException e) {
                            throw new HiveException("Failed to read a file: "
                                    + fileIO.getFile().getAbsolutePath(), e);
                        }
                        if(bytesRead == 0) { // reached file EOF
                            break;
                        }
                        assert (bytesRead > 0) : bytesRead;

                        // reads training examples from a buffer
                        inputBuf.flip();
                        int remain = inputBuf.remaining();
                        if(remain < INT_BYTES) {
                            throw new HiveException("Illegal file format was detected");
                        }
                        while(remain >= INT_BYTES) {
                            int recordBytes = inputBuf.getInt();
                            remain -= INT_BYTES;
                            if(remain < recordBytes) {
                                break;
                            }

                            final int xLength = inputBuf.getInt();
                            final Feature[] x = new Feature[xLength];
                            for(int j = 0; j < xLength; j++) {
                                x[j] = new Feature(inputBuf);
                            }
                            double y = inputBuf.getDouble();

                            // invoke training
                            ++_t;
                            train(x, y);

                            remain -= recordBytes;
                        }
                        inputBuf.compact();
                    }

                }
                logger.info("Performed " + i + " iterations of "
                        + NumberUtils.formatNumber(numTrainingExamples)
                        + " training examples (thus " + NumberUtils.formatNumber(_t)
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
            this._inputBuf = null;
            this._fileIO = null;
        }
    }

    @Nullable
    private static Feature[] parseFeatures(@Nonnull final Object arg, @Nonnull final ListObjectInspector listOI)
            throws HiveException {
        if(arg == null) {
            return null;
        }
        final int length = listOI.getListLength(arg);
        final Feature[] ary = new Feature[length];
        int j = 0;
        for(int i = 0; i < length; i++) {
            Object o = listOI.getListElement(arg, i);
            if(o == null) {
                continue;
            }
            String s = o.toString();
            Feature f = Feature.parse(s);
            ary[j] = f;
            j++;
        }
        if(j == length) {
            return ary;
        } else {
            Feature[] dst = new Feature[j];
            System.arraycopy(ary, 0, dst, 0, j);
            return dst;
        }
    }

    public static final class Feature {
        int index;
        double value;

        public Feature(int index, double value) {
            this.index = index;
            this.value = value;
        }

        public Feature(@Nonnull ByteBuffer src) {
            readFrom(src);
        }

        @Nonnull
        static Feature parse(@Nonnull final String s) throws HiveException {
            int pos = s.indexOf(":");
            String s1 = s.substring(0, pos);
            String s2 = s.substring(pos + 1);
            int index = Integer.parseInt(s1);
            if(index < 0) {
                throw new HiveException("Feature index MUST be greater than 0: " + s);
            }
            double value = Double.parseDouble(s2);
            return new Feature(index, value);
        }

        static void parse(@Nonnull final String s, @Nonnull final Feature probe)
                throws HiveException {
            int pos = s.indexOf(":");
            String s1 = s.substring(0, pos);
            String s2 = s.substring(pos + 1);
            int index = Integer.parseInt(s1);
            if(index < 0) {
                throw new HiveException("Feature index MUST be greater than 0: " + s);
            }
            double value = Double.parseDouble(s2);
            probe.index = index;
            probe.value = value;
        }

        int bytes() {
            return (Integer.SIZE + Double.SIZE) / 8;
        }

        void writeTo(@Nonnull final ByteBuffer dst) {
            dst.putInt(index);
            dst.putDouble(value);
        }

        void readFrom(@Nonnull final ByteBuffer src) {
            this.index = src.getInt();
            this.value = src.getDouble();
        }

    }

    private static int requiredBytes(@Nonnull final Feature[] x) {
        int ret = 0;
        for(Feature f : x) {
            assert (f != null);
            ret += f.bytes();
        }
        return ret;
    }
}
