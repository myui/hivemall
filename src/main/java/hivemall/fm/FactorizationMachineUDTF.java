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
import hivemall.common.EtaEstimator;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
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
        train(x, y);
    }

    public void train(@Nonnull final Feature[] x, final double y) throws HiveException {
        // check
        _model.check(x);

        final float eta = _etaEstimator.eta(_t);
        final double dlossMultiplier = _model.dloss(x, y);

        // w0 update
        _model.updateW0(x, dlossMultiplier, eta);

        for(Feature e : x) {
            if(e == null) {
                continue;
            }
            int i = e.index;
            double xi = e.value;
            // wi update
            _model.updateWi(x, dlossMultiplier, i, xi, eta);
            for(int f = 0, k = _factor; f < k; f++) {
                // Vif update
                _model.updateV(x, dlossMultiplier, i, f, eta);
            }
        }
    }

    @Override
    public void close() throws HiveException {
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

    @Nullable
    private static Feature[] parseFeatures(@Nonnull final Object arg, @Nonnull final ListObjectInspector listOI)
            throws HiveException {
        if(arg == null) {
            return null;
        }
        final int length = listOI.getListLength(arg);
        final Feature[] ary = new Feature[length];
        for(int i = 0; i < length; i++) {
            Object o = listOI.getListElement(arg, i);
            if(o == null) {
                continue;
            }
            String s = o.toString();
            Feature f = Feature.parse(s);
            ary[i] = f;
        }
        return ary;
    }

    public static final class Feature {
        int index;
        double value;

        public Feature(int index, double value) {
            this.index = index;
            this.value = value;
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
    }

}
