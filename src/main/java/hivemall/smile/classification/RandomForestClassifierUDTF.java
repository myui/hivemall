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
import hivemall.smile.utils.SmileExtUtils;
import hivemall.smile.utils.SmileTaskExecutor;
import hivemall.smile.vm.StackMachine;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.MapredContextAccessor;
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
import smile.math.Math;
import smile.math.Random;

@Description(name = "train_randomforest_classifier", value = "_FUNC_(double[] features, int label [, string options]) - "
        + "Returns a relation consists of <string pred_model, double[] var_importance>")
public final class RandomForestClassifierUDTF extends UDTFWithOptions {
    private static final Log logger = LogFactory.getLog(RandomForestClassifierUDTF.class);

    private ListObjectInspector featureListOI;
    private PrimitiveObjectInspector featureElemOI;
    private PrimitiveObjectInspector labelOI;

    private List<double[]> featuresList;
    private IntArrayList labels;
    /**
     * The number of trees for each task
     */
    private int numTrees;
    /**
     * The number of random selected features
     */
    private int numVars;
    private long seed;
    private Attribute[] attributes;
    private OutputType outputType;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("trees", "num_trees", true, "The number of trees for each task [default: 50]");
        opts.addOption("vars", "num_variables", true, "The number of random selected features [default: floor(sqrt(dim))]");
        opts.addOption("seed", true, "seed value in long [default: -1 (random)]");
        opts.addOption("attrs", "attribute_types", true, "Comma separated attribute types "
                + "(Q for quantative variable and C for categorical variable. e.g., [Q,C,Q,C])");
        opts.addOption("output", "output_type", true, "The output type (opscode/vm or javascript/js) [default: opscode]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        int T = 50, M = -1;
        Attribute[] attrs = null;
        long seed = -1L;
        String output = "opscode";

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);

            T = Primitives.parseInt(cl.getOptionValue("num_trees"), T);
            if(T < 1) {
                throw new IllegalArgumentException("Invlaid number of trees: " + T);
            }
            M = Primitives.parseInt(cl.getOptionValue("num_variables"), M);
            attrs = SmileExtUtils.resolveAttributes(cl.getOptionValue("attribute_types"));
            seed = Primitives.parseLong(cl.getOptionValue("seed"), seed);
            output = cl.getOptionValue("output", output);
        }

        this.numTrees = T;
        this.numVars = M;
        this.seed = seed;
        this.attributes = attrs;
        this.outputType = OutputType.resolve(output);

        return cl;
    }

    public enum OutputType {
        opscode, javascript;

        public static OutputType resolve(String name) {
            if("opscode".equalsIgnoreCase(name) || "vm".equalsIgnoreCase(name)) {
                return opscode;
            } else if("javascript".equalsIgnoreCase(name) || "js".equalsIgnoreCase(name)) {
                return javascript;
            }
            throw new IllegalStateException("Unexpected output type: " + name);
        }

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
        fieldNames.add("oob_errors");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("oob_tests");
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

        // run training
        train(x, y, attributes, numTrees, numVars, seed);

        // clean up
        this.featureListOI = null;
        this.featureElemOI = null;
        this.labelOI = null;
        this.attributes = null;
    }

    /**
     * @param x
     *            features
     * @param y
     *            label
     * @param attrs
     *            attribute types
     * @param numTrees
     *            The number of trees
     * @param numVars
     *            The number of variables to pick up in each node.
     * @param seed
     *            The seed number for Random Forest
     */
    private void train(@Nonnull final double[][] x, @Nonnull final int[] y, final Attribute[] attrs, final int numTrees, final int numVars, final long seed)
            throws HiveException {
        if(x.length != y.length) {
            throw new HiveException(String.format("The sizes of X and Y don't match: %d != %d", x.length, y.length));
        }
        int[] labels = SmileExtUtils.classLables(y);
        Attribute[] attributes = SmileExtUtils.attributeTypes(attrs, x);
        int numInputVars = (numVars == -1) ? (int) Math.floor(Math.sqrt(x[0].length)) : numVars;

        final int numExamples = x.length;
        int[][] prediction = new int[numExamples][labels.length]; // placeholder for out-of-bag prediction
        int[][] order = SmileExtUtils.sort(attributes, x);
        AtomicInteger remainingTasks = new AtomicInteger(numTrees);
        List<TrainingTask> tasks = new ArrayList<TrainingTask>();
        for(int i = 0; i < numTrees; i++) {
            tasks.add(new TrainingTask(this, attributes, x, y, numInputVars, order, prediction, seed
                    + i, remainingTasks));
        }

        MapredContext mapredContext = MapredContextAccessor.get();
        final SmileTaskExecutor executor = new SmileTaskExecutor(mapredContext);
        try {
            executor.run(tasks);
        } catch (Exception ex) {
            throw new HiveException(ex);
        } finally {
            executor.shotdown();
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

    /**
     * Trains a regression tree.
     */
    private static final class TrainingTask implements Callable<Integer> {
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
        private final int numVars;
        /**
         * The out-of-bag predictions.
         */
        private final int[][] prediction;

        private final RandomForestClassifierUDTF udtf;
        private final long seed;
        private final AtomicInteger remainingTasks;

        /**
         * Constructor.
         */
        TrainingTask(RandomForestClassifierUDTF udtf, Attribute[] attributes, double[][] x, int[] y, int M, int[][] order, int[][] prediction, long seed, AtomicInteger remainingTasks) {
            this.udtf = udtf;
            this.attributes = attributes;
            this.x = x;
            this.y = y;
            this.order = order;
            this.numVars = M;
            this.prediction = prediction;
            this.seed = seed;
            this.remainingTasks = remainingTasks;
        }

        @Override
        public Integer call() throws HiveException {
            long s = (this.seed == -1L) ? Thread.currentThread().getId()
                    * System.currentTimeMillis() : this.seed;
            final Random random = new Random(s);
            final int n = x.length;
            int[] samples = new int[n]; // Training samples draw with replacement.
            for(int i = 0; i < n; i++) {
                samples[random.nextInt(n)]++;
            }

            DecisionTree tree = new DecisionTree(attributes, x, y, numVars, samples, order);

            // out-of-bag prediction
            for(int i = 0; i < n; i++) {
                if(samples[i] == 0) {
                    final int p = tree.predict(x[i]);
                    synchronized(prediction[i]) {
                        prediction[i][p]++;
                    }
                }
            }

            String model = getModel(tree, udtf.outputType);
            //model = model.replaceAll("\n", "\\\\n");
            double[] importance = tree.importance();
            int remain = remainingTasks.decrementAndGet();
            boolean lastTask = (remain == 0);
            udtf.forward(model, importance, y, prediction, lastTask);

            return Integer.valueOf(remain);
        }

        private String getModel(DecisionTree tree, OutputType outputType) {
            final String model;
            switch (outputType) {
                case opscode: {
                    model = tree.predictOpCodegen(StackMachine.SEP);
                    break;
                }
                case javascript: {
                    model = tree.predictCodegen();
                    break;
                }
                default: {
                    logger.warn("Unexpected output type: " + udtf.outputType
                            + ". Use javascript for the output instead");
                    model = tree.predictCodegen();
                    break;
                }
            }
            return model;
        }

    }

}
