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
import hivemall.smile.ModelType;
import hivemall.smile.classification.DecisionTree.SplitRule;
import hivemall.smile.data.Attribute;
import hivemall.smile.utils.SmileExtUtils;
import hivemall.smile.utils.SmileTaskExecutor;
import hivemall.smile.vm.StackMachine;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.compress.Base91;
import hivemall.utils.compress.DeflateCodec;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.io.IOUtils;
import hivemall.utils.lang.Primitives;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
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

@Description(
        name = "train_randomforest_classifier",
        value = "_FUNC_(double[] features, int label [, string options]) - "
                + "Returns a relation consists of "
                + "<int model_id, int model_type, string pred_model, array<double> var_importance, int oob_errors, int oob_tests>")
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
     * The maximum number of the tree depth
     */
    private int _maxDepth;
    /**
     * The maximum number of leaf nodes
     */
    private int _maxLeafNodes;
    private int _minSamplesSplit;
    private int _minSamplesLeaf;
    private long _seed;
    private Attribute[] _attributes;
    private ModelType _outputType;
    private SplitRule _splitRule;

    @Override
    protected Options getOptions() {
        Options opts = new Options();
        opts.addOption("trees", "num_trees", true,
            "The number of trees for each task [default: 50]");
        opts.addOption("vars", "num_variables", true,
            "The number of random selected features [default: ceil(sqrt(x[0].length))]."
                    + " int(num_variables * x[0].length) is considered if num_variable is (0,1]");
        opts.addOption("depth", "max_depth", true,
            "The maximum number of the tree depth [default: Integer.MAX_VALUE]");
        opts.addOption("leafs", "max_leaf_nodes", true,
            "The maximum number of leaf nodes [default: Integer.MAX_VALUE]");
        opts.addOption("splits", "min_split", true,
            "A node that has greater than or equals to `min_split` examples will split [default: 2]");
        opts.addOption("min_samples_leaf", true,
            "The minimum number of samples in a leaf node [default: 1]");
        opts.addOption("seed", true, "seed value in long [default: -1 (random)]");
        opts.addOption("attrs", "attribute_types", true, "Comma separated attribute types "
                + "(Q for quantitative variable and C for categorical variable. e.g., [Q,C,Q,C])");
        opts.addOption("output", "output_type", true,
            "The output type (serialization/ser or opscode/vm or javascript/js) [default: serialization]");
        opts.addOption("rule", "split_rule", true, "Split algorithm [default: GINI, ENTROPY]");
        opts.addOption("disable_compression", false,
            "Whether to disable compression of the output script [default: false]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        int trees = 50, maxDepth = Integer.MAX_VALUE;
        int numLeafs = Integer.MAX_VALUE, minSplits = 2, minSamplesLeaf = 1;
        float numVars = -1.f;
        Attribute[] attrs = null;
        long seed = -1L;
        String output = "serialization";
        SplitRule splitRule = SplitRule.GINI;
        boolean compress = true;

        CommandLine cl = null;
        if (argOIs.length >= 3) {
            String rawArgs = HiveUtils.getConstString(argOIs[2]);
            cl = parseOptions(rawArgs);

            trees = Primitives.parseInt(cl.getOptionValue("num_trees"), trees);
            if (trees < 1) {
                throw new IllegalArgumentException("Invlaid number of trees: " + trees);
            }
            numVars = Primitives.parseFloat(cl.getOptionValue("num_variables"), numVars);
            maxDepth = Primitives.parseInt(cl.getOptionValue("max_depth"), maxDepth);
            numLeafs = Primitives.parseInt(cl.getOptionValue("max_leaf_nodes"), numLeafs);
            minSplits = Primitives.parseInt(cl.getOptionValue("min_split"), minSplits);
            minSamplesLeaf = Primitives.parseInt(cl.getOptionValue("min_samples_leaf"),
                minSamplesLeaf);
            seed = Primitives.parseLong(cl.getOptionValue("seed"), seed);
            attrs = SmileExtUtils.resolveAttributes(cl.getOptionValue("attribute_types"));
            output = cl.getOptionValue("output", output);
            splitRule = SmileExtUtils.resolveSplitRule(cl.getOptionValue("split_rule", "GINI"));
            if (cl.hasOption("disable_compression")) {
                compress = false;
            }
        }

        this._numTrees = trees;
        this._numVars = numVars;
        this._maxDepth = maxDepth;
        this._maxLeafNodes = numLeafs;
        this._minSamplesSplit = minSplits;
        this._minSamplesLeaf = minSamplesLeaf;
        this._seed = seed;
        this._attributes = attrs;
        this._outputType = ModelType.resolve(output, compress);
        this._splitRule = splitRule;

        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2 && argOIs.length != 3) {
            throw new UDFArgumentException(
                getClass().getSimpleName()
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

        ArrayList<String> fieldNames = new ArrayList<String>(6);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(6);

        fieldNames.add("model_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        fieldNames.add("model_type");
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
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
        if (args[0] == null) {
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

        if (numExamples > 0) {
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
        if (_minSamplesSplit <= 0) {
            throw new HiveException("Invalid minSamplesSplit: " + _minSamplesSplit);
        }
        if (_maxDepth < 1) {
            throw new HiveException("Invalid maxDepth: " + _maxDepth);
        }
    }

    /**
     * @param x features
     * @param y label
     * @param attrs attribute types
     * @param numTrees The number of trees
     * @param numVars The number of variables to pick up in each node.
     * @param seed The seed number for Random Forest
     */
    private void train(@Nonnull final double[][] x, @Nonnull final int[] y) throws HiveException {
        if (x.length != y.length) {
            throw new HiveException(String.format("The sizes of X and Y don't match: %d != %d",
                x.length, y.length));
        }
        checkOptions();

        // Shuffle training samples    
        SmileExtUtils.shuffle(x, y, _seed);

        int[] labels = SmileExtUtils.classLables(y);
        Attribute[] attributes = SmileExtUtils.attributeTypes(_attributes, x);
        int numInputVars = SmileExtUtils.computeNumInputVars(_numVars, x);

        if (logger.isInfoEnabled()) {
            logger.info("numTrees: " + _numTrees + ", numVars: " + numInputVars + ", maxDepth: "
                    + _maxDepth + ", minSamplesSplit: " + _minSamplesSplit + ", maxLeafs: "
                    + _maxLeafNodes + ", splitRule: " + _splitRule + ", seed: " + _seed);
        }

        final int numExamples = x.length;
        int[][] prediction = new int[numExamples][labels.length]; // placeholder for out-of-bag prediction
        int[][] order = SmileExtUtils.sort(attributes, x);
        AtomicInteger remainingTasks = new AtomicInteger(_numTrees);
        List<TrainingTask> tasks = new ArrayList<TrainingTask>();
        for (int i = 0; i < _numTrees; i++) {
            long s = (_seed == -1L) ? -1L : _seed + i;
            tasks.add(new TrainingTask(this, i, attributes, x, y, numInputVars, order, prediction,
                s, remainingTasks));
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
    synchronized void forward(final int modelId, @Nonnull final Text model,
            @Nonnull final double[] importance, final int[] y, final int[][] prediction,
            final boolean lastTask) throws HiveException {
        int oobErrors = 0;
        int oobTests = 0;
        if (lastTask) {
            // out-of-bag error estimate
            for (int i = 0; i < y.length; i++) {
                final int pred = smile.math.Math.whichMax(prediction[i]);
                if (prediction[i][pred] > 0) {
                    oobTests++;
                    if (pred != y[i]) {
                        oobErrors++;
                    }
                }
            }
        }
        Object[] forwardObjs = new Object[6];
        forwardObjs[0] = new IntWritable(modelId);
        forwardObjs[1] = new IntWritable(_outputType.getId());
        forwardObjs[2] = model;
        forwardObjs[3] = WritableUtils.toWritableList(importance);
        forwardObjs[4] = new IntWritable(oobErrors);
        forwardObjs[5] = new IntWritable(oobTests);
        forward(forwardObjs);

        logger.info("Forworded " + modelId + "-th DecisionTree out of " + _numTrees);
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
         * The index of training values in ascending order. Note that only numeric attributes will
         * be sorted.
         */
        private final int[][] _order;
        /**
         * The number of variables to pick up in each node.
         */
        private final int _numVars;
        /**
         * The out-of-bag predictions.
         */
        private final int[][] _prediction;

        private final RandomForestClassifierUDTF _udtf;
        private final int _taskId;
        private final long _seed;
        private final AtomicInteger _remainingTasks;

        TrainingTask(RandomForestClassifierUDTF udtf, int taskId, Attribute[] attributes,
                double[][] x, int[] y, int numVars, int[][] order, int[][] prediction, long seed,
                AtomicInteger remainingTasks) {
            this._udtf = udtf;
            this._taskId = taskId;
            this._attributes = attributes;
            this._x = x;
            this._y = y;
            this._order = order;
            this._numVars = numVars;
            this._prediction = prediction;
            this._seed = seed;
            this._remainingTasks = remainingTasks;
        }

        @Override
        public Integer call() throws HiveException {
            long s = (this._seed == -1L) ? SmileExtUtils.generateSeed() : new smile.math.Random(
                _seed).nextLong();
            final smile.math.Random rnd1 = new smile.math.Random(s);
            final smile.math.Random rnd2 = new smile.math.Random(rnd1.nextLong());
            final int N = _x.length;

            // Training samples draw with replacement.
            final int[] bags = new int[N];
            final BitSet sampled = new BitSet(N);
            for (int i = 0; i < N; i++) {
                int index = rnd1.nextInt(N);
                bags[i] = index;
                sampled.set(index);
            }

            DecisionTree tree = new DecisionTree(_attributes, _x, _y, _numVars, _udtf._maxDepth,
                _udtf._maxLeafNodes, _udtf._minSamplesSplit, _udtf._minSamplesLeaf, bags, _order,
                _udtf._splitRule, rnd2);

            // out-of-bag prediction
            for (int i = sampled.nextClearBit(0); i < N; i = sampled.nextClearBit(i + 1)) {
                final int p = tree.predict(_x[i]);
                synchronized (_prediction[i]) {
                    _prediction[i][p]++;
                }
            }

            Text model = getModel(tree, _udtf._outputType);
            double[] importance = tree.importance();
            int remain = _remainingTasks.decrementAndGet();
            boolean lastTask = (remain == 0);
            _udtf.forward(_taskId + 1, model, importance, _y, _prediction, lastTask);

            return Integer.valueOf(remain);
        }

        private static Text getModel(@Nonnull final DecisionTree tree,
                @Nonnull final ModelType outputType) throws HiveException {
            final Text model;
            switch (outputType) {
                case serialization:
                case serialization_compressed: {
                    byte[] b = tree.predictSerCodegen(outputType.isCompressed());
                    b = Base91.encode(b);
                    model = new Text(b);
                    break;
                }
                case opscode:
                case opscode_compressed: {
                    String s = tree.predictOpCodegen(StackMachine.SEP);
                    if (outputType.isCompressed()) {
                        byte[] b = s.getBytes();
                        final DeflateCodec codec = new DeflateCodec(true, false);
                        try {
                            b = codec.compress(b);
                        } catch (IOException e) {
                            throw new HiveException("Failed to compressing a model", e);
                        } finally {
                            IOUtils.closeQuietly(codec);
                        }
                        b = Base91.encode(b);
                        model = new Text(b);
                    } else {
                        model = new Text(s);
                    }
                    break;
                }
                case javascript:
                case javascript_compressed: {
                    String s = tree.predictJsCodegen();
                    if (outputType.isCompressed()) {
                        byte[] b = s.getBytes();
                        final DeflateCodec codec = new DeflateCodec(true, false);
                        try {
                            b = codec.compress(b);
                        } catch (IOException e) {
                            throw new HiveException("Failed to compressing a model", e);
                        } finally {
                            IOUtils.closeQuietly(codec);
                        }
                        b = Base91.encode(b);
                        model = new Text(b);
                    } else {
                        model = new Text(s);
                    }
                    break;
                }
                default:
                    throw new HiveException("Unexpected output type: " + outputType
                            + ". Use javascript for the output instead");
            }
            return model;
        }

    }

}
