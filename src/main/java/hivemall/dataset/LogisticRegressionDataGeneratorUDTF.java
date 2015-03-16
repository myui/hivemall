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
package hivemall.dataset;

import hivemall.UDTFWithOptions;
import hivemall.utils.hadoop.HadoopUtils;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "lr_datagen", value = "_FUNC_(options string) - Generates a logistic regression dataset")
public final class LogisticRegressionDataGeneratorUDTF extends UDTFWithOptions {

    private static final int N_BUFFERS = 1000;

    // control variable
    private int position;
    private float[] labels;
    private String[][] featuresArray;
    private Float[][] featuresFloatArray;

    private int n_examples;
    private int n_features;
    private int n_dimensions;
    private float eps;
    private float prob_one;
    private int r_seed;
    private boolean dense;
    private boolean sort;
    private boolean classification;

    private Random rnd1 = null, rnd2 = null;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("ne", "n_examples", true, "Number of training examples created for each task [DEFAULT: 1000]");
        opts.addOption("nf", "n_features", true, "Number of features contained for each example [DEFAULT: 10]");
        opts.addOption("nd", "n_dims", true, "The size of feature dimensions [DEFAULT: 200]");
        opts.addOption("eps", true, "eps Epsilon factor by which positive examples are scaled [DEFAULT: 3.0]");
        opts.addOption("p1", "prob_one", true, " Probability in [0, 1.0) that a label is 1 [DEFAULT: 0.6]");
        opts.addOption("seed", true, "The seed value for random number generator [DEFAULT: 43]");
        opts.addOption("dense", false, "Make a dense dataset or not. If not specified, a sparse dataset is generated.\n"
                + "For sparse, n_dims should be much larger than n_features. When disabled, n_features must be equals to n_dims ");
        opts.addOption("sort", false, "Sort features if specified (used only for sparse dataset)");
        opts.addOption("cl", "classification", false, "Toggle this option on to generate a classification dataset");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 1) {
            throw new UDFArgumentException("Expected number of arguments is 1: " + argOIs.length);
        }
        String opts = HiveUtils.getConstString(argOIs[0]);
        CommandLine cl = parseOptions(opts);

        this.n_examples = NumberUtils.parseInt(cl.getOptionValue("n_examples"), 1000);
        this.n_features = NumberUtils.parseInt(cl.getOptionValue("n_features"), 10);
        this.n_dimensions = NumberUtils.parseInt(cl.getOptionValue("n_dims"), 200);
        this.eps = Primitives.parseFloat(cl.getOptionValue("eps"), 3.f);
        this.prob_one = Primitives.parseFloat(cl.getOptionValue("prob_one"), 0.6f);
        this.r_seed = Primitives.parseInt(cl.getOptionValue("seed"), 43);
        this.dense = cl.hasOption("dense");
        this.sort = cl.hasOption("sort");
        this.classification = cl.hasOption("classification");

        if(n_features > n_dimensions) {
            throw new UDFArgumentException("n_features '" + n_features
                    + "' should be greater than or equals to n_dimensions '" + n_dimensions + "'");
        }
        if(dense) {
            if(n_features != n_dimensions) {
                throw new UDFArgumentException("n_features '" + n_features
                        + "' must be equlas to n_dimensions '" + n_dimensions
                        + "' when making a dense dataset");
            }
        }

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        processOptions(argOIs);

        init();

        ArrayList<String> fieldNames = new ArrayList<String>(2);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
        fieldNames.add("label");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
        fieldNames.add("features");
        if(dense) {
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaFloatObjectInspector));
        } else {
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private void init() {
        this.labels = new float[N_BUFFERS];
        if(dense) {
            this.featuresFloatArray = new Float[N_BUFFERS][n_features];
        } else {
            this.featuresArray = new String[N_BUFFERS][n_features];
        }
        this.position = 0;
    }

    @Override
    public void process(Object[] argOIs) throws HiveException {
        if(rnd1 == null) {
            assert (rnd2 == null);
            int taskid = HadoopUtils.getTaskId();
            int seed = r_seed + taskid;
            this.rnd1 = new Random(seed);
            this.rnd2 = new Random(seed + 1);
        }
        for(int i = 0; i < n_examples; i++) {
            if(dense) {
                generateDenseData();
            } else {
                generateSparseData();
            }
            position++;
            if(position == N_BUFFERS) {
                flushBuffered(position);
                this.position = 0;
            }
        }
    }

    private void generateSparseData() throws HiveException {
        float label = rnd1.nextFloat();
        float sign = (label >= prob_one) ? 1.f : 0.f;
        labels[position] = classification ? sign : label;
        String[] features = featuresArray[position];
        assert (features != null);
        final BitSet used = new BitSet(n_dimensions);
        int searchClearBitsFrom = 0;
        for(int i = 0, retry = 0; i < n_features; i++) {
            int f = rnd2.nextInt(n_dimensions);
            if(used.get(f)) {
                if(retry < 3) {
                    --i;
                    ++retry;
                    continue;
                }
                searchClearBitsFrom = used.nextClearBit(searchClearBitsFrom);
                f = searchClearBitsFrom;
            }
            used.set(f);
            float w = (float) rnd2.nextGaussian() + (sign * eps);
            String y = f + ":" + w;
            features[i] = y;
            retry = 0;
        }
        if(sort) {
            Arrays.sort(features, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    int i1 = Integer.parseInt(o1.split(":")[0]);
                    int i2 = Integer.parseInt(o2.split(":")[0]);
                    return Primitives.compare(i1, i2);
                }
            });
        }
    }

    private void generateDenseData() {
        float label = rnd1.nextFloat();
        float sign = (label >= prob_one) ? 1.f : 0.f;
        labels[position] = classification ? sign : label;
        Float[] features = featuresFloatArray[position];
        assert (features != null);
        for(int i = 0; i < n_features; i++) {
            float w = (float) rnd2.nextGaussian() + (sign * eps);
            features[i] = Float.valueOf(w);
        }
    }

    private void flushBuffered(int position) throws HiveException {
        final Object[] forwardObjs = new Object[2];
        if(dense) {
            for(int i = 0; i < position; i++) {
                forwardObjs[0] = Float.valueOf(labels[i]);
                forwardObjs[1] = Arrays.asList(featuresFloatArray[i]);
                forward(forwardObjs);
            }
        } else {
            for(int i = 0; i < position; i++) {
                forwardObjs[0] = Float.valueOf(labels[i]);
                forwardObjs[1] = Arrays.asList(featuresArray[i]);
                forward(forwardObjs);
            }
        }
    }

    @Override
    public void close() throws HiveException {
        if(position > 0) {
            flushBuffered(position);
        }
        // release resources to help GCs
        this.labels = null;
        this.featuresArray = null;
        this.featuresFloatArray = null;
    }

}
