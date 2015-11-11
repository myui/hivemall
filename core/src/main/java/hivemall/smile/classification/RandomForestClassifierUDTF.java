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
import hivemall.smile.classification.DecisionTree.SplitRule;
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

@Description(name = "train_randomforest_classifier", value = "_FUNC_(double[] features, int label [, string options]) - "
        + "Returns a relation consists of <string pred_model, double[] var_importance, int oob_errors, int oob_tests>")
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
    private int _numTrees;
    /**
     * The number of random selected features
     */
    private float _numVars;
    /**
     * The maximum number of leaf nodes
     */
    private int _maxLeafNodes;
    private int _minSamplesSplit;
    private long _seed;
    private Attribute[] _attributes;
    private OutputType _outputType;
    private SplitRule _splitRule;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("trees", "num_trees", true, "The number of trees for each task [default: 50]");
        opts.addOption("vars", "num_variables", true, "The number of random selected features [default: round(max(sqrt(x[0].length),x[0].length/3.0))]."
                + " If a floating number is specified, int(num_variables * x[0].length) is considered if num_variable is (0,1]");
        opts.addOption("leafs", "max_leaf_nodes", true, "The maximum number of leaf nodes [default: Integer.MAX_VALUE]");
        opts.addOption("splits", "min_split", true, "A node that has greater than or equals to `min_split` examples will split [default: 2]");
        opts.addOption("seed", true, "seed value in long [default: -1 (random)]");
        opts.addOption("attrs", "attribute_types", true, "Comma separated attribute types "
                + "(Q for quantitative variable and C for categorical variable. e.g., [Q,C,Q,C])");
        opts.addOption("output", "output_type", true, "The output type (opscode/vm or javascript/js) [default: opscode]");
        opts.addOption("rule", "split_rule", true, "Split algorithm [default: GINI, ENTROPY]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        int T = 50, J = Integer.MAX_VALUE, S = 2;
        float M = -1.f;
        Attribute[] attrs = null;
        long seed = -1L;
        String output = "opscode";
        SplitRule splitRule = SplitRule.GINI;

        CommandLine cl = null;
        if(argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);

            T = Primitives.parseInt(cl.getOptionValue("num_trees"), T);
            if(T < 1) {
                throw new IllegalArgumentException("Invlaid number of trees: " + T);
            }
            M = Primitives.parseFloat(cl.getOptionValue("num_variables"), M);
            J = Primitives.parseInt(cl.getOptionValue("max_leaf_nodes"), J);
            S = Primitives.parseInt(cl.getOptionValue("min_split"), S);
            seed = Primitives.parseLong(cl.getOptionValue("seed"), seed);
            attrs = SmileExtUtils.resolveAttributes(cl.getOptionValue("attribute_types"));
            output = cl.getOptionValue("output", output);
            splitRule = SmileExtUtils.resolveSplitRule(cl.getOptionValue("split_rule", "GINI"));
        }

        this._numTrees = T;
        this._numVars = M;
        this._maxLeafNodes = J;
        this._minSamplesSplit = S;
        this._seed = seed;
        this._attributes = attrs;
        this._outputType = OutputType.resolve(output);
        this._splitRule = splitRule;

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

        if(numExamples > 0) {
            double[][] x = featuresList.toArray(new double[numExamples][]);
            this.featuresList = null;
            int[] y = labels.toArray();
            this.labels = null;

            // run training
            train(x, y);
        }

        // clean up
        this.featureListOI = null;
        this.featureElemOI = null;
        this.labelOI = null;
        this._attributes = null;
    }

    private void checkOptions() throws HiveException {
        if(_minSamplesSplit <= 0) {
            throw new HiveException("Invalid minSamplesSplit: " + _minSamplesSplit);
        }
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
    private void train(@Nonnull final double[][] x, @Nonnull final int[] y) throws HiveException {
        if(x.length != y.length) {
            throw new HiveException(String.format("The sizes of X and Y don't match: %d != %d", x.length, y.length));
        }
        checkOptions();

        // Shuffle training samples    
        SmileExtUtils.shuffle(x, y, _seed);

        int[] labels = SmileExtUtils.classLables(y);
        Attribute[] attributes = SmileExtUtils.attributeTypes(_attributes, x);
        int numInputVars = SmileExtUtils.computeNumInputVars(_numVars, x);

        if(logger.isInfoEnabled()) {
            logger.info("numTrees: " + _numTrees + ", numVars: " + numInputVars
                    + ", minSamplesSplit: " + _minSamplesSplit + ", maxLeafs: " + _maxLeafNodes
                    + ", splitRule: " + _splitRule + ", seed: " + _seed);
        }

        final int numExamples = x.length;
        int[][] prediction = new int[numExamples][labels.length]; // placeholder for out-of-bag prediction
        int[][] order = SmileExtUtils.sort(attributes, x);
        AtomicInteger remainingTasks = new AtomicInteger(_numTrees);
        List<TrainingTask> tasks = new ArrayList<TrainingTask>();
        for(int i = 0; i < _numTrees; i++) {
            long s = (_seed == -1L) ? -1L : _seed + i;
            tasks.add(new TrainingTask(this, attributes, x, y, numInputVars, _maxLeafNodes, _minSamplesSplit, order, prediction, _splitRule, s, remainingTasks));
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
     * Synchronized because {@link #forward(Object)} should be called from a single thread.
     */
    synchronized void forward(@Nonnull final String model, @Nonnull final double[] importance, final int[] y, final int[][] prediction, final boolean lastTask)
            throws HiveException {
        int oobErrors = 0;
        int oobTests = 0;
        if(lastTask) {
            // out-of-bag error estimate
            for(int i = 0; i < y.length; i++) {
                int pred = smile.math.Math.whichMax(prediction[i]);
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
        private final Attribute[] _attributes;
        /**
         * Training instances.
         */
        private final double[][] _x;
        /**
         * Training sample labels.
         */
        private final int[] _y;
        /**
         * The index of training values in ascending order. Note that only
         * numeric attributes will be sorted.
         */
        private final int[][] _order;
        /**
         * Split rule of DecisionTrere.
         */
        private final SplitRule _splitRule;
        /**
         * The number of variables to pick up in each node.
         */
        private final int _numVars;
        /**
         * The maximum number of leaf nodes in the tree.
         */
        private final int _maxLeafs;
        /**
         * The number of instances in a node below which the tree will not split.
         */
        private final int _minSamplesSplit;
        /**
         * The out-of-bag predictions.
         */
        private final int[][] _prediction;

        private final RandomForestClassifierUDTF _udtf;
        private final long _seed;
        private final AtomicInteger _remainingTasks;

        TrainingTask(RandomForestClassifierUDTF udtf, Attribute[] attributes, double[][] x, int[] y, int numVars, int maxLeafs, int minSamplesSplit, int[][] order, int[][] prediction, SplitRule splitRule, long seed, AtomicInteger remainingTasks) {
            this._udtf = udtf;
            this._attributes = attributes;
            this._x = x;
            this._y = y;
            this._order = order;
            this._splitRule = splitRule;
            this._numVars = numVars;
            this._maxLeafs = maxLeafs;
            this._minSamplesSplit = minSamplesSplit;
            this._prediction = prediction;
            this._seed = seed;
            this._remainingTasks = remainingTasks;
        }

        @Override
        public Integer call() throws HiveException {
            long s = (this._seed == -1L) ? SmileExtUtils.generateSeed()
                    : new smile.math.Random(_seed).nextLong();
            final smile.math.Random rnd1 = new smile.math.Random(s);
            final smile.math.Random rnd2 = new smile.math.Random(rnd1.nextLong());
            final int n = _x.length;
            // Training samples draw with replacement.
            final int[] samples = new int[n];
            for(int i = 0; i < n; i++) {
                samples[rnd1.nextInt(n)]++;
            }

            DecisionTree tree = new DecisionTree(_attributes, _x, _y, _numVars, _maxLeafs, _minSamplesSplit, samples, _order, _splitRule, rnd2);

            // out-of-bag prediction
            for(int i = 0; i < n; i++) {
                if(samples[i] == 0) {
                    final int p = tree.predict(_x[i]);
                    synchronized(_prediction[i]) {
                        _prediction[i][p]++;
                    }
                }
            }

            String model = getModel(tree, _udtf._outputType);
            double[] importance = tree.importance();
            int remain = _remainingTasks.decrementAndGet();
            boolean lastTask = (remain == 0);
            _udtf.forward(model, importance, _y, _prediction, lastTask);

            return Integer.valueOf(remain);
        }

        private static String getModel(@Nonnull final DecisionTree tree, @Nonnull final OutputType outputType) {
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
                    logger.warn("Unexpected output type: " + outputType
                            + ". Use javascript for the output instead");
                    model = tree.predictCodegen();
                    break;
                }
            }
            return model;
        }

    }

}
