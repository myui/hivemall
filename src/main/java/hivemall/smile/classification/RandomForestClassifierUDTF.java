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
package hivemall.smile.classification;

import hivemall.UDTFWithOptions;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import smile.data.Attribute;
import smile.data.NumericAttribute;
import smile.math.Math;
import smile.math.Random;
import smile.util.MulticoreExecutor;
import smile.util.SmileUtils;

@Description(name = "train_randomforest_classifier", value = "_FUNC_(double[] features, int label [, string options]) - "
        + "Returns a relation consists of <string pred_model, double[] var_importance>")
public final class RandomForestClassifierUDTF extends UDTFWithOptions {

    private ListObjectInspector featureListOI;
    private PrimitiveObjectInspector featureElemOI;
    private PrimitiveObjectInspector labelOI;

    private ArrayList<double[]> featuresList;
    private IntArrayList labels;
    private int T, M;
    private Attribute[] attributes;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("T", "num_trees", true, "The number of trees for each task [default: 50]");
        opts.addOption("M", "num_features", true, "The number of random selected features [default: floor(sqrt(dim))]");
        opts.addOption("attr_types", "attribute_types", true, "Comma separated attribute types"
                + "(Q for quantative variable and C for categorical variable. e.g., [Q,C,Q,C])");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        int T = 50, M = -1;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);
        }

        if(T < 1) {
            throw new IllegalArgumentException("Invlaid number of trees: " + T);
        }
        if(M < 1) {
            throw new IllegalArgumentException("Invalid number of variables for splitting: " + M);
        }

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(getClass().getSimpleName()
                    + " takes 2 or 3 arguments: double[] features, int label [, const string options]: "
                    + argOIs.length);
        }

        ListObjectInspector listOI = HiveUtils.asListOI(argOIs[0]);
        ObjectInspector elemOI = listOI.getListElementObjectInspector();
        this.featureListOI = listOI;
        this.featureElemOI = HiveUtils.asDoubleCompatibleOI(elemOI);
        this.labelOI = HiveUtils.asIntCompatibleOI(argOIs[1]);

        processOptions(argOIs);

        this.featuresList = new ArrayList<double[]>(1024);
        this.labels = new IntArrayList(1024);

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("pred_model");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        fieldNames.add("var_importance");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        fieldNames.add("oobErrors");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("oobTests");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if(args[0] == null) {
            throw new HiveException("array<double> features was null");
        }
        double[] features = HiveUtils.asDoubleArray(args[0], featureListOI, featureElemOI);
        int label = PrimitiveObjectInspectorUtils.getInt(args[1], labelOI);

        featuresList.add(features);
        labels.add(label);
    }

    @Override
    public void close() throws HiveException {
        int numExamples = featuresList.size();
        double[][] x = featuresList.toArray(new double[numExamples][]);
        this.featuresList = null;
        int[] y = labels.toArray();
        this.labels = null;
        train(x, y, attributes, T, M);

        // clean up
        this.featureListOI = null;
        this.featureElemOI = null;
        this.labelOI = null;
        this.attributes = null;
    }

    private void train(@Nonnull final double[][] x, @Nonnull final int[] y, final Attribute[] attrs, final int T, final int M)
            throws HiveException {
        if(x.length != y.length) {
            throw new HiveException(String.format("The sizes of X and Y don't match: %d != %d", x.length, y.length));
        }
        int[] labels = classLables(y);
        Attribute[] attributes = attributeTypes(attrs, x);

        final int numExamples = x.length;
        int[][] prediction = new int[numExamples][labels.length]; // out-of-bag prediction
        int[][] order = SmileUtils.sort(attributes, x);
        AtomicInteger remainingTasks = new AtomicInteger(T);
        List<TrainingTask> tasks = new ArrayList<TrainingTask>();
        for(int i = 0; i < T; i++) {
            tasks.add(new TrainingTask(this, attributes, x, y, M, order, prediction, remainingTasks));
        }

        try {
            MulticoreExecutor.run(tasks);
        } catch (Exception ex) {
            throw new HiveException(ex);
        }
    }

    /**
     * Synchronized because {@link #forward(Object)} should be called from a
     * single thread.
     */
    public synchronized void forward(@Nonnull final String model, @Nonnull final double[] importance, final int[] y, final int[][] prediction, final boolean lastTask)
            throws HiveException {
        int oobErrors = 0;
        int oobTests = 0;
        if(lastTask) {
            // out-of-bag error estimate
            for(int i = 0; i < y.length; i++) {
                int pred = Math.whichMax(prediction[i]);
                if(prediction[i][pred] > 0) {
                    oobTests++;
                    if(pred != y[i]) {
                        oobErrors++;
                    }
                }
            }
        }
        Object[] forwardObjs = new Object[4];
        forwardObjs[0] = new Text(model);
        forwardObjs[1] = WritableUtils.toWritableList(importance);
        forwardObjs[2] = new IntWritable(oobErrors);
        forwardObjs[3] = new IntWritable(oobTests);
        forward(forwardObjs);
    }

    @Nonnull
    public static int[] classLables(@Nonnull final int[] y) throws HiveException {
        final int[] labels = Math.unique(y);
        Arrays.sort(labels);

        if(labels.length < 2) {
            throw new HiveException("Only one class.");
        }
        for(int i = 0; i < labels.length; i++) {
            if(labels[i] < 0) {
                throw new HiveException("Negative class label: " + labels[i]);
            }
            if(i > 0 && labels[i] - labels[i - 1] > 1) {
                throw new HiveException("Missing class: " + labels[i] + 1);
            }
        }

        return labels;
    }

    public static Attribute[] attributeTypes(@Nullable Attribute[] attributes, @Nonnull final double[][] x) {
        if(attributes == null) {
            int p = x[0].length;
            attributes = new Attribute[p];
            for(int i = 0; i < p; i++) {
                attributes[i] = new NumericAttribute("V" + (i + 1));
            }
        }
        return attributes;
    }

    /**
     * Trains a regression tree.
     */
    private static final class TrainingTask implements Callable<DecisionTree> {
        /**
         * Attribute properties.
         */
        private final Attribute[] attributes;
        /**
         * Training instances.
         */
        private final double[][] x;
        /**
         * Training sample labels.
         */
        private final int[] y;
        /**
         * The index of training values in ascending order. Note that only
         * numeric attributes will be sorted.
         */
        private final int[][] order;
        /**
         * The number of variables to pick up in each node.
         */
        private final int M;
        /**
         * The out-of-bag predictions.
         */
        private final int[][] prediction;

        private final RandomForestClassifierUDTF udtf;
        private final AtomicInteger remainingTasks;

        /**
         * Constructor.
         */
        TrainingTask(RandomForestClassifierUDTF udtf, Attribute[] attributes, double[][] x, int[] y, int M, int[][] order, int[][] prediction, AtomicInteger remainingTasks) {
            this.udtf = udtf;
            this.attributes = attributes;
            this.x = x;
            this.y = y;
            this.order = order;
            this.M = M;
            this.prediction = prediction;
            this.remainingTasks = remainingTasks;
        }

        @Override
        public DecisionTree call() throws HiveException {
            int n = x.length;
            Random random = new Random(Thread.currentThread().getId() * System.currentTimeMillis());
            int[] samples = new int[n]; // Training samples draw with replacement.
            for(int i = 0; i < n; i++) {
                samples[random.nextInt(n)]++;
            }

            DecisionTree tree = new DecisionTree(attributes, x, y, M, samples, order);

            // out-of-bag prediction
            for(int i = 0; i < n; i++) {
                if(samples[i] == 0) {
                    int p = tree.predict(x[i]);
                    synchronized(prediction[i]) {
                        prediction[i][p]++;
                    }
                }
            }

            String model = tree.predictCodegen();
            double[] importance = tree.importance();
            int remain = remainingTasks.decrementAndGet();
            boolean lastTask = (remain == 0);
            udtf.forward(model, importance, y, prediction, lastTask);

            return tree;
        }
    }

}
