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
package hivemall.smile.regression;

import hivemall.UDTFWithOptions;
import hivemall.smile.utils.SmileExtUtils;
import hivemall.smile.utils.SmileTaskExecutor;
import hivemall.smile.vm.StackMachine;
import hivemall.utils.collections.DoubleArrayList;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.MapredContextAccessor;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
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

@Description(name = "train_randomforest_regression", value = "_FUNC_(double[] features, int label [, string options]) - "
        + "Returns a relation consists of <string pred_model, double[] var_importance, int oob_errors, int oob_tests>")
public final class RandomForestRegressionUDTF extends UDTFWithOptions {
    private static final Log logger = LogFactory.getLog(RandomForestRegressionUDTF.class);

    private ListObjectInspector featureListOI;
    private PrimitiveObjectInspector featureElemOI;
    private PrimitiveObjectInspector targetOI;

    private List<double[]> featuresList;
    private DoubleArrayList targets;
    /**
     * The number of trees for each task
     */
    private int numTrees;
    /**
     * The number of random selected features
     */
    private int numVars;
    /**
     * The maximum number of leaf nodes
     */
    private int maxLeafNodes;
    private int minSamplesSplit;
    private long seed;
    private Attribute[] attributes;
    private OutputType outputType;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("trees", "num_trees", true, "The number of trees for each task [default: 50]");
        opts.addOption("vars", "num_variables", true, "The number of random selected features [default: max(1, x[0].length / 3)]");
        opts.addOption("leafs", "max_leaf_nodes", true, "The maximum number of leaf nodes [default: Integer.MAX_VALUE]");
        opts.addOption("split", "min_samples_split", true, "S number of instances in a node below which the tree will not split [default: 5]");
        opts.addOption("seed", true, "seed value in long [default: -1 (random)]");
        opts.addOption("attrs", "attribute_types", true, "Comma separated attribute types "
                + "(Q for quantative variable and C for categorical variable. e.g., [Q,C,Q,C])");
        opts.addOption("output", "output_type", true, "The output type (opscode/vm or javascript/js) [default: opscode]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        int T = 50, M = -1, J = Integer.MAX_VALUE, S = 5;
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
            J = Primitives.parseInt(cl.getOptionValue("max_leaf_nodes"), J);
            S = Primitives.parseInt(cl.getOptionValue("min_samples_split"), S);
            seed = Primitives.parseLong(cl.getOptionValue("seed"), seed);
            attrs = SmileExtUtils.resolveAttributes(cl.getOptionValue("attribute_types"));
            output = cl.getOptionValue("output", output);
        }

        this.numTrees = T;
        this.numVars = M;
        this.maxLeafNodes = J;
        this.minSamplesSplit = S;
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
        this.targetOI = HiveUtils.asDoubleCompatibleOI(argOIs[1]);

        processOptions(argOIs);

        this.featuresList = new ArrayList<double[]>(1024);
        this.targets = new DoubleArrayList(1024);

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("pred_model");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        fieldNames.add("var_importance");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        fieldNames.add("oob_errors");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
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
        double target = PrimitiveObjectInspectorUtils.getDouble(args[1], targetOI);

        featuresList.add(features);
        targets.add(target);
    }

    @Override
    public void close() throws HiveException {
        int numExamples = featuresList.size();

        if(numExamples > 0) {
            double[][] x = featuresList.toArray(new double[numExamples][]);
            this.featuresList = null;
            double[] y = targets.toArray();
            this.targets = null;

            // run training
            train(x, y, attributes, numTrees, numVars, maxLeafNodes, minSamplesSplit, seed);
        }

        // clean up
        this.featureListOI = null;
        this.featureElemOI = null;
        this.targetOI = null;
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
    private void train(@Nonnull final double[][] x, @Nonnull final double[] y, @Nullable final Attribute[] attrs, final int numTrees, final int numVars, final int maxLeafs, final int minSamplesSplit, final long seed)
            throws HiveException {
        if(x.length != y.length) {
            throw new HiveException(String.format("The sizes of X and Y don't match: %d != %d", x.length, y.length));
        }
        if(minSamplesSplit <= 0) {
            throw new HiveException("Invalid minSamplesSplit: " + minSamplesSplit);
        }
        // Shuffle training samples 
        SmileExtUtils.shuffle(x, y, seed);

        Attribute[] attributes = SmileExtUtils.attributeTypes(attrs, x);
        int numInputVars = (numVars <= 0) ? Math.max(1, x[0].length / 3) : numVars;

        if(logger.isInfoEnabled()) {
            logger.info("numTrees: " + numTrees + ", numVars: " + numInputVars
                    + ", minSamplesSplit: " + minSamplesSplit + ", maxLeafs: " + maxLeafs
                    + ", nodeCapacity: " + minSamplesSplit + ", seed: " + seed);
        }

        final int numExamples = x.length;
        double[] prediction = new double[numExamples]; // placeholder for out-of-bag prediction
        int[] oob = new int[numExamples];
        int[][] order = SmileExtUtils.sort(attributes, x);
        AtomicInteger remainingTasks = new AtomicInteger(numTrees);
        List<TrainingTask> tasks = new ArrayList<TrainingTask>();
        for(int i = 0; i < numTrees; i++) {
            long s = (seed == -1L) ? -1L : seed + i;
            tasks.add(new TrainingTask(this, attributes, x, y, numInputVars, maxLeafs, minSamplesSplit, order, prediction, oob, s, remainingTasks));
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
    public synchronized void forward(@Nonnull final String model, @Nonnull final double[] importance, final double[] y, final double[] prediction, final int[] oob, final boolean lastTask)
            throws HiveException {
        double oobErrors = 0.d;
        int oobTests = 0;
        if(lastTask) {
            // out-of-bag error estimate
            for(int i = 0; i < y.length; i++) {
                if(oob[i] > 0) {
                    oobTests++;
                    double pred = prediction[i] / oob[i];
                    oobErrors += Math.sqr(pred - y[i]);
                }
            }
        }
        Object[] forwardObjs = new Object[4];
        forwardObjs[0] = new Text(model);
        forwardObjs[1] = WritableUtils.toWritableList(importance);
        forwardObjs[2] = new DoubleWritable(oobErrors);
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
        private final double[] y;
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
         * The maximum number of leaf nodes in the tree.
         */
        private final int numLeafs;
        /**
         * The number of instances in a node below which the tree will not split
         */
        private final int minSamplesSplit;
        /**
         * The out-of-bag predictions.
         */
        private final double[] prediction;
        /**
         * Out-of-bag sample
         */
        private final int[] oob;

        private final RandomForestRegressionUDTF udtf;
        private final long seed;
        private final AtomicInteger remainingTasks;

        TrainingTask(RandomForestRegressionUDTF udtf, Attribute[] attributes, double[][] x, double[] y, int numVars, int numLeafs, int minSamplesSplit, int[][] order, double[] prediction, int[] oob, long seed, AtomicInteger remainingTasks) {
            this.udtf = udtf;
            this.attributes = attributes;
            this.x = x;
            this.y = y;
            this.order = order;
            this.numVars = numVars;
            this.numLeafs = numLeafs;
            this.minSamplesSplit = minSamplesSplit;
            this.prediction = prediction;
            this.oob = oob;
            this.seed = seed;
            this.remainingTasks = remainingTasks;
        }

        @Override
        public Integer call() throws HiveException {
            long s = (this.seed == -1L) ? SmileExtUtils.generateSeed() : this.seed;
            final Random rand = new Random(s);
            final int n = x.length;
            // Training samples draw with replacement.
            int[] samples = new int[n];
            for(int i = 0; i < n; i++) {
                samples[rand.nextInt(n)]++;
            }

            RegressionTree tree = new RegressionTree(attributes, x, y, numVars, numLeafs, minSamplesSplit, order, samples, s);

            // out-of-bag prediction
            for(int i = 0; i < n; i++) {
                if(samples[i] == 0) {
                    double pred = tree.predict(x[i]);
                    synchronized(x[i]) {
                        prediction[i] += pred;
                        oob[i]++;
                    }
                }
            }

            String model = getModel(tree, udtf.outputType);
            double[] importance = tree.importance();
            int remain = remainingTasks.decrementAndGet();
            boolean lastTask = (remain == 0);
            udtf.forward(model, importance, y, prediction, oob, lastTask);

            return Integer.valueOf(remain);
        }

        private String getModel(@Nonnull final RegressionTree tree, @Nonnull final OutputType outputType) {
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
