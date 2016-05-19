/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
/*
 * Copyright (c) 2010 Haifeng Li
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.smile.classification;

import hivemall.smile.data.Attribute;
import hivemall.smile.data.Attribute.AttributeType;
import hivemall.smile.utils.SmileExtUtils;
import hivemall.utils.collections.IntArrayList;
import hivemall.utils.lang.ObjectUtils;
import hivemall.utils.lang.StringUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import smile.classification.Classifier;
import smile.math.Math;
import smile.math.Random;

/**
 * Decision tree for classification. A decision tree can be learned by splitting the training set
 * into subsets based on an attribute value test. This process is repeated on each derived subset in
 * a recursive manner called recursive partitioning. The recursion is completed when the subset at a
 * node all has the same value of the target variable, or when splitting no longer adds value to the
 * predictions.
 * <p>
 * The algorithms that are used for constructing decision trees usually work top-down by choosing a
 * variable at each step that is the next best variable to use in splitting the set of items. "Best"
 * is defined by how well the variable splits the set into homogeneous subsets that have the same
 * value of the target variable. Different algorithms use different formulae for measuring "best".
 * Used by the CART algorithm, Gini impurity is a measure of how often a randomly chosen element
 * from the set would be incorrectly labeled if it were randomly labeled according to the
 * distribution of labels in the subset. Gini impurity can be computed by summing the probability of
 * each item being chosen times the probability of a mistake in categorizing that item. It reaches
 * its minimum (zero) when all cases in the node fall into a single target category. Information
 * gain is another popular measure, used by the ID3, C4.5 and C5.0 algorithms. Information gain is
 * based on the concept of entropy used in information theory. For categorical variables with
 * different number of levels, however, information gain are biased in favor of those attributes
 * with more levels. Instead, one may employ the information gain ratio, which solves the drawback
 * of information gain.
 * <p>
 * Classification and Regression Tree techniques have a number of advantages over many of those
 * alternative techniques.
 * <dl>
 * <dt>Simple to understand and interpret.</dt>
 * <dd>In most cases, the interpretation of results summarized in a tree is very simple. This
 * simplicity is useful not only for purposes of rapid classification of new observations, but can
 * also often yield a much simpler "model" for explaining why observations are classified or
 * predicted in a particular manner.</dd>
 * <dt>Able to handle both numerical and categorical data.</dt>
 * <dd>Other techniques are usually specialized in analyzing datasets that have only one type of
 * variable.</dd>
 * <dt>Tree methods are nonparametric and nonlinear.</dt>
 * <dd>The final results of using tree methods for classification or regression can be summarized in
 * a series of (usually few) logical if-then conditions (tree nodes). Therefore, there is no
 * implicit assumption that the underlying relationships between the predictor variables and the
 * dependent variable are linear, follow some specific non-linear link function, or that they are
 * even monotonic in nature. Thus, tree methods are particularly well suited for data mining tasks,
 * where there is often little a priori knowledge nor any coherent set of theories or predictions
 * regarding which variables are related and how. In those types of data analytics, tree methods can
 * often reveal simple relationships between just a few variables that could have easily gone
 * unnoticed using other analytic techniques.</dd>
 * </dl>
 * One major problem with classification and regression trees is their high variance. Often a small
 * change in the data can result in a very different series of splits, making interpretation
 * somewhat precarious. Besides, decision-tree learners can create over-complex trees that cause
 * over-fitting. Mechanisms such as pruning are necessary to avoid this problem. Another limitation
 * of trees is the lack of smoothness of the prediction surface.
 * <p>
 * Some techniques such as bagging, boosting, and random forest use more than one decision tree for
 * their analysis.
 */
public final class DecisionTree implements Classifier<double[]> {
    /**
     * The attributes of independent variable.
     */
    private final Attribute[] _attributes;
    private final boolean _hasNumericType;
    /**
     * Variable importance. Every time a split of a node is made on variable the (GINI, information
     * gain, etc.) impurity criterion for the two descendant nodes is less than the parent node.
     * Adding up the decreases for each individual variable over the tree gives a simple measure of
     * variable importance.
     */
    private final double[] _importance;
    /**
     * The root of the regression tree
     */
    private final Node _root;
    /**
     * The maximum number of the tree depth
     */
    private final int _maxDepth;
    /**
     * The splitting rule.
     */
    private final SplitRule _rule;
    /**
     * The number of classes.
     */
    private final int _k;
    /**
     * The number of input variables to be used to determine the decision at a node of the tree.
     */
    private final int _numVars;
    /**
     * The number of instances in a node below which the tree will not split.
     */
    private final int _minSplit;
    /**
     * The minimum number of samples in a leaf node
     */
    private final int _minLeafSize;
    /**
     * The index of training values in ascending order. Note that only numeric attributes will be
     * sorted.
     */
    private final int[][] _order;

    private final Random _rnd;

    /**
     * The criterion to choose variable to split instances.
     */
    public static enum SplitRule {
        /**
         * Used by the CART algorithm, Gini impurity is a measure of how often a randomly chosen
         * element from the set would be incorrectly labeled if it were randomly labeled according
         * to the distribution of labels in the subset. Gini impurity can be computed by summing the
         * probability of each item being chosen times the probability of a mistake in categorizing
         * that item. It reaches its minimum (zero) when all cases in the node fall into a single
         * target category.
         */
        GINI,
        /**
         * Used by the ID3, C4.5 and C5.0 tree generation algorithms.
         */
        ENTROPY,
        /**
         * Classification error.
         */
        CLASSIFICATION_ERROR
    }

    /**
     * Classification tree node.
     */
    public static final class Node implements Externalizable {

        /**
         * Predicted class label for this node.
         */
        int output = -1;
        /**
         * The split feature for this node.
         */
        int splitFeature = -1;
        /**
         * The type of split feature
         */
        AttributeType splitFeatureType = null;
        /**
         * The split value.
         */
        double splitValue = Double.NaN;
        /**
         * Reduction in splitting criterion.
         */
        double splitScore = 0.0;
        /**
         * Children node.
         */
        Node trueChild = null;
        /**
         * Children node.
         */
        Node falseChild = null;
        /**
         * Predicted output for children node.
         */
        int trueChildOutput = -1;
        /**
         * Predicted output for children node.
         */
        int falseChildOutput = -1;

        public Node() {}// for Externalizable

        /**
         * Constructor.
         */
        public Node(int output) {
            this.output = output;
        }

        /**
         * Evaluate the regression tree over an instance.
         */
        public int predict(final double[] x) {
            if (trueChild == null && falseChild == null) {
                return output;
            } else {
                if (splitFeatureType == AttributeType.NOMINAL) {
                    if (x[splitFeature] == splitValue) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                } else if (splitFeatureType == AttributeType.NUMERIC) {
                    if (x[splitFeature] <= splitValue) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                } else {
                    throw new IllegalStateException("Unsupported attribute type: "
                            + splitFeatureType);
                }
            }
        }

        public void jsCodegen(@Nonnull final StringBuilder builder, final int depth) {
            if (trueChild == null && falseChild == null) {
                indent(builder, depth);
                builder.append("").append(output).append(";\n");
            } else {
                if (splitFeatureType == AttributeType.NOMINAL) {
                    indent(builder, depth);
                    builder.append("if(x[")
                           .append(splitFeature)
                           .append("] == ")
                           .append(splitValue)
                           .append(") {\n");
                    trueChild.jsCodegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("} else {\n");
                    falseChild.jsCodegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("}\n");
                } else if (splitFeatureType == AttributeType.NUMERIC) {
                    indent(builder, depth);
                    builder.append("if(x[")
                           .append(splitFeature)
                           .append("] <= ")
                           .append(splitValue)
                           .append(") {\n");
                    trueChild.jsCodegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("} else  {\n");
                    falseChild.jsCodegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("}\n");
                } else {
                    throw new IllegalStateException("Unsupported attribute type: "
                            + splitFeatureType);
                }
            }
        }

        public int opCodegen(final List<String> scripts, int depth) {
            int selfDepth = 0;
            final StringBuilder buf = new StringBuilder();
            if (trueChild == null && falseChild == null) {
                buf.append("push ").append(output);
                scripts.add(buf.toString());
                buf.setLength(0);
                buf.append("goto last");
                scripts.add(buf.toString());
                selfDepth += 2;
            } else {
                if (splitFeatureType == AttributeType.NOMINAL) {
                    buf.append("push ").append("x[").append(splitFeature).append("]");
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("push ").append(splitValue);
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("ifeq ");
                    scripts.add(buf.toString());
                    depth += 3;
                    selfDepth += 3;
                    int trueDepth = trueChild.opCodegen(scripts, depth);
                    selfDepth += trueDepth;
                    scripts.set(depth - 1, "ifeq " + String.valueOf(depth + trueDepth));
                    int falseDepth = falseChild.opCodegen(scripts, depth + trueDepth);
                    selfDepth += falseDepth;
                } else if (splitFeatureType == AttributeType.NUMERIC) {
                    buf.append("push ").append("x[").append(splitFeature).append("]");
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("push ").append(splitValue);
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("ifle ");
                    scripts.add(buf.toString());
                    depth += 3;
                    selfDepth += 3;
                    int trueDepth = trueChild.opCodegen(scripts, depth);
                    selfDepth += trueDepth;
                    scripts.set(depth - 1, "ifle " + String.valueOf(depth + trueDepth));
                    int falseDepth = falseChild.opCodegen(scripts, depth + trueDepth);
                    selfDepth += falseDepth;
                } else {
                    throw new IllegalStateException("Unsupported attribute type: "
                            + splitFeatureType);
                }
            }
            return selfDepth;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(output);
            out.writeInt(splitFeature);
            if (splitFeatureType == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(splitFeatureType.getTypeId());
            }
            out.writeDouble(splitValue);
            if (trueChild == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                trueChild.writeExternal(out);
            }
            if (falseChild == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                falseChild.writeExternal(out);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.output = in.readInt();
            this.splitFeature = in.readInt();
            int typeId = in.readInt();
            if (typeId == -1) {
                this.splitFeatureType = null;
            } else {
                this.splitFeatureType = AttributeType.resolve(typeId);
            }
            this.splitValue = in.readDouble();
            if (in.readBoolean()) {
                this.trueChild = new Node();
                trueChild.readExternal(in);
            }
            if (in.readBoolean()) {
                this.falseChild = new Node();
                falseChild.readExternal(in);
            }
        }

    }

    private static void indent(final StringBuilder builder, final int depth) {
        for (int i = 0; i < depth; i++) {
            builder.append("  ");
        }
    }

    /**
     * Classification tree node for training purpose.
     */
    private final class TrainNode implements Comparable<TrainNode> {
        /**
         * The associated regression tree node.
         */
        final Node node;
        /**
         * Training dataset.
         */
        final double[][] x;
        /**
         * class labels.
         */
        final int[] y;

        int[] bags;

        final int depth;

        /**
         * Constructor.
         */
        public TrainNode(Node node, double[][] x, int[] y, int[] bags, int depth) {
            this.node = node;
            this.x = x;
            this.y = y;
            this.bags = bags;
            this.depth = depth;
        }

        @Override
        public int compareTo(TrainNode a) {
            return (int) Math.signum(a.node.splitScore - node.splitScore);
        }

        /**
         * Finds the best attribute to split on at the current node.
         * 
         * @return true if a split exists to reduce squared error, false otherwise.
         */
        public boolean findBestSplit() {
            // avoid split if tree depth is larger than threshold
            if (depth >= _maxDepth) {
                return false;
            }
            // avoid split if the number of samples is less than threshold
            final int numSamples = bags.length;
            if (numSamples <= _minSplit) {
                return false;
            }

            // Sample count in each class.
            final int[] count = new int[_k];
            final boolean pure = sampleCount(count);

            // Since all instances have same label, stop splitting.
            if (pure) {
                return false;
            }

            final double impurity = impurity(count, numSamples, _rule);

            final int p = _attributes.length;
            final int[] variableIndex = new int[p];
            for (int i = 0; i < p; i++) {
                variableIndex[i] = i;
            }
            if (_numVars < p) {
                SmileExtUtils.shuffle(variableIndex, _rnd);
            }

            final int[] samples = _hasNumericType ? SmileExtUtils.bagsToSamples(bags, x.length)
                    : null;
            final int[] falseCount = new int[_k];
            for (int j = 0; j < _numVars; j++) {
                Node split = findBestSplit(numSamples, count, falseCount, impurity,
                    variableIndex[j], samples);
                if (split.splitScore > node.splitScore) {
                    node.splitFeature = split.splitFeature;
                    node.splitFeatureType = split.splitFeatureType;
                    node.splitValue = split.splitValue;
                    node.splitScore = split.splitScore;
                    node.trueChildOutput = split.trueChildOutput;
                    node.falseChildOutput = split.falseChildOutput;
                }
            }

            return (node.splitFeature != -1);
        }

        private boolean sampleCount(@Nonnull final int[] count) {
            int label = -1;
            boolean pure = true;
            for (int i = 0; i < bags.length; i++) {
                int index = bags[i];
                int y_i = y[index];
                count[y_i]++;

                if (label == -1) {
                    label = y_i;
                } else if (y_i != label) {
                    pure = false;
                }
            }
            return pure;
        }

        /**
         * Finds the best split cutoff for attribute j at the current node.
         * 
         * @param n the number instances in this node.
         * @param count the sample count in each class.
         * @param falseCount an array to store sample count in each class for false child node.
         * @param impurity the impurity of this node.
         * @param j the attribute index to split on.
         */
        private Node findBestSplit(final int n, final int[] count, final int[] falseCount,
                final double impurity, final int j, @Nullable final int[] samples) {
            final Node splitNode = new Node();

            if (_attributes[j].type == AttributeType.NOMINAL) {
                final int m = _attributes[j].getSize();
                final int[][] trueCount = new int[m][_k];

                for (int i = 0, size = bags.length; i < size; i++) {
                    int index = bags[i];
                    int x_ij = (int) x[index][j];
                    trueCount[x_ij][y[index]]++;
                }

                for (int l = 0; l < m; l++) {
                    final int tc = Math.sum(trueCount[l]);
                    final int fc = n - tc;

                    // skip splitting this feature.
                    if (tc < _minSplit || fc < _minSplit) {
                        continue;
                    }

                    for (int q = 0; q < _k; q++) {
                        falseCount[q] = count[q] - trueCount[l][q];
                    }

                    final double gain = impurity - (double) tc / n
                            * impurity(trueCount[l], tc, _rule) - (double) fc / n
                            * impurity(falseCount, fc, _rule);

                    if (gain > splitNode.splitScore) {
                        // new best split
                        splitNode.splitFeature = j;
                        splitNode.splitFeatureType = AttributeType.NOMINAL;
                        splitNode.splitValue = l;
                        splitNode.splitScore = gain;
                        splitNode.trueChildOutput = Math.whichMax(trueCount[l]);
                        splitNode.falseChildOutput = Math.whichMax(falseCount);
                    }
                }
            } else if (_attributes[j].type == AttributeType.NUMERIC) {
                final int[] trueCount = new int[_k];
                double prevx = Double.NaN;
                int prevy = -1;

                assert (samples != null);
                for (final int i : _order[j]) {
                    final int sample = samples[i];
                    if (sample > 0) {
                        final double x_ij = x[i][j];
                        final int y_i = y[i];

                        if (Double.isNaN(prevx) || x_ij == prevx || y_i == prevy) {
                            prevx = x_ij;
                            prevy = y_i;
                            trueCount[y_i] += sample;
                            continue;
                        }

                        final int tc = Math.sum(trueCount);
                        final int fc = n - tc;

                        // skip splitting this feature.
                        if (tc < _minSplit || fc < _minSplit) {
                            prevx = x_ij;
                            prevy = y_i;
                            trueCount[y_i] += sample;
                            continue;
                        }

                        for (int l = 0; l < _k; l++) {
                            falseCount[l] = count[l] - trueCount[l];
                        }

                        final double gain = impurity - (double) tc / n
                                * impurity(trueCount, tc, _rule) - (double) fc / n
                                * impurity(falseCount, fc, _rule);

                        if (gain > splitNode.splitScore) {
                            // new best split
                            splitNode.splitFeature = j;
                            splitNode.splitFeatureType = AttributeType.NUMERIC;
                            splitNode.splitValue = (x_ij + prevx) / 2.d;
                            splitNode.splitScore = gain;
                            splitNode.trueChildOutput = Math.whichMax(trueCount);
                            splitNode.falseChildOutput = Math.whichMax(falseCount);
                        }

                        prevx = x_ij;
                        prevy = y_i;
                        trueCount[y_i] += sample;
                    }
                }
            } else {
                throw new IllegalStateException("Unsupported attribute type: "
                        + _attributes[j].type);
            }

            return splitNode;
        }

        /**
         * Split the node into two children nodes. Returns true if split success.
         */
        public boolean split(@Nullable final PriorityQueue<TrainNode> nextSplits) {
            if (node.splitFeature < 0) {
                throw new IllegalStateException("Split a node with invalid feature.");
            }

            // split sample bags
            int childBagSize = (int) (bags.length * 0.4);
            IntArrayList trueBags = new IntArrayList(childBagSize);
            IntArrayList falseBags = new IntArrayList(childBagSize);
            int tc = splitSamples(trueBags, falseBags);
            int fc = bags.length - tc;
            this.bags = null; // help GC for recursive call

            if (tc < _minLeafSize || fc < _minLeafSize) {
                // set the node as leaf                
                node.splitFeature = -1;
                node.splitFeatureType = null;
                node.splitValue = Double.NaN;
                node.splitScore = 0.0;
                return false;
            }

            node.trueChild = new Node(node.trueChildOutput);
            TrainNode trueChild = new TrainNode(node.trueChild, x, y, trueBags.toArray(), depth + 1);
            trueBags = null; // help GC for recursive call
            if (tc >= _minSplit && trueChild.findBestSplit()) {
                if (nextSplits != null) {
                    nextSplits.add(trueChild);
                } else {
                    trueChild.split(null);
                }
            }

            node.falseChild = new Node(node.falseChildOutput);
            TrainNode falseChild = new TrainNode(node.falseChild, x, y, falseBags.toArray(),
                depth + 1);
            falseBags = null; // help GC for recursive call
            if (fc >= _minSplit && falseChild.findBestSplit()) {
                if (nextSplits != null) {
                    nextSplits.add(falseChild);
                } else {
                    falseChild.split(null);
                }
            }

            _importance[node.splitFeature] += node.splitScore;

            return true;
        }

        /**
         * @return the number of true samples
         */
        private int splitSamples(@Nonnull final IntArrayList trueBags,
                @Nonnull final IntArrayList falseBags) {
            int tc = 0;
            if (node.splitFeatureType == AttributeType.NOMINAL) {
                final int splitFeature = node.splitFeature;
                final double splitValue = node.splitValue;
                for (int i = 0, size = bags.length; i < size; i++) {
                    final int index = bags[i];
                    if (x[index][splitFeature] == splitValue) {
                        trueBags.add(index);
                        tc++;
                    } else {
                        falseBags.add(index);
                    }
                }
            } else if (node.splitFeatureType == AttributeType.NUMERIC) {
                final int splitFeature = node.splitFeature;
                final double splitValue = node.splitValue;
                for (int i = 0, size = bags.length; i < size; i++) {
                    final int index = bags[i];
                    if (x[index][splitFeature] <= splitValue) {
                        trueBags.add(index);
                        tc++;
                    } else {
                        falseBags.add(index);
                    }
                }
            } else {
                throw new IllegalStateException("Unsupported attribute type: "
                        + node.splitFeatureType);
            }
            return tc;
        }

    }


    /**
     * Returns the impurity of a node.
     * 
     * @param count the sample count in each class.
     * @param n the number of samples in the node.
     * @param rule the rule for splitting a node.
     * @return the impurity of a node
     */
    private static double impurity(@Nonnull final int[] count, final int n,
            @Nonnull final SplitRule rule) {
        double impurity = 0.0;

        switch (rule) {
            case GINI: {
                impurity = 1.0;
                for (int i = 0; i < count.length; i++) {
                    if (count[i] > 0) {
                        double p = (double) count[i] / n;
                        impurity -= p * p;
                    }
                }
                break;
            }
            case ENTROPY: {
                for (int i = 0; i < count.length; i++) {
                    if (count[i] > 0) {
                        double p = (double) count[i] / n;
                        impurity -= p * Math.log2(p);
                    }
                }
                break;
            }
            case CLASSIFICATION_ERROR: {
                impurity = 0.d;
                for (int i = 0; i < count.length; i++) {
                    if (count[i] > 0) {
                        impurity = Math.max(impurity, (double) count[i] / n);
                    }
                }
                impurity = Math.abs(1.d - impurity);
                break;
            }
        }

        return impurity;
    }

    public DecisionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull int[] y,
            int numLeafs) {
        this(attributes, x, y, x[0].length, Integer.MAX_VALUE, numLeafs, 2, 1, null, null, SplitRule.GINI, null);
    }

    public DecisionTree(@Nullable Attribute[] attributes, @Nullable double[][] x,
            @Nullable int[] y, int numLeafs, @Nullable smile.math.Random rand) {
        this(attributes, x, y, x[0].length, Integer.MAX_VALUE, numLeafs, 2, 1, null, null, SplitRule.GINI, rand);
    }

    /**
     * Constructor. Learns a classification tree for random forest.
     *
     * @param attributes the attribute properties.
     * @param x the training instances.
     * @param y the response variable.
     * @param numVars the number of input variables to pick to split on at each node. It seems that
     *        dim/3 give generally good performance, where dim is the number of variables.
     * @param maxLeafs the maximum number of leaf nodes in the tree.
     * @param minSplits the number of minimum elements in a node to split
     * @param minLeafSize the minimum size of leaf nodes.
     * @param order the index of training values in ascending order. Note that only numeric
     *        attributes need be sorted.
     * @param bags the sample set of instances for stochastic learning.
     * @param rule the splitting rule.
     * @param seed
     */
    public DecisionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull int[] y,
            int numVars, int maxDepth, int maxLeafs, int minSplits, int minLeafSize,
            @Nullable int[] bags, @Nullable int[][] order, @Nonnull SplitRule rule,
            @Nullable smile.math.Random rand) {
        checkArgument(x, y, numVars, maxDepth, maxLeafs, minSplits, minLeafSize);

        this._k = Math.max(y) + 1;
        if (_k < 2) {
            throw new IllegalArgumentException("Only one class or negative class labels.");
        }

        this._attributes = SmileExtUtils.attributeTypes(attributes, x);
        if (attributes.length != x[0].length) {
            throw new IllegalArgumentException("-attrs option is invliad: "
                    + Arrays.toString(attributes));
        }
        this._hasNumericType = SmileExtUtils.containsNumericType(_attributes);

        this._numVars = numVars;
        this._maxDepth = maxDepth;
        this._minSplit = minSplits;
        this._minLeafSize = minLeafSize;
        this._rule = rule;
        this._order = (order == null) ? SmileExtUtils.sort(_attributes, x) : order;
        this._importance = new double[_attributes.length];
        this._rnd = (rand == null) ? new smile.math.Random() : rand;

        final int n = y.length;
        final int[] count = new int[_k];
        if (bags == null) {
            bags = new int[n];
            for (int i = 0; i < n; i++) {
                bags[i] = i;
                count[y[i]]++;
            }
        } else {
            for (int i = 0; i < n; i++) {
                int index = bags[i];
                count[y[index]]++;
            }
        }

        this._root = new Node(Math.whichMax(count));

        final TrainNode trainRoot = new TrainNode(_root, x, y, bags, 1);
        if (maxLeafs == Integer.MAX_VALUE) {
            if (trainRoot.findBestSplit()) {
                trainRoot.split(null);
            }
        } else {
            // Priority queue for best-first tree growing.
            final PriorityQueue<TrainNode> nextSplits = new PriorityQueue<TrainNode>();
            // Now add splits to the tree until max tree size is reached
            if (trainRoot.findBestSplit()) {
                nextSplits.add(trainRoot);
            }
            // Pop best leaf from priority queue, split it, and push
            // children nodes into the queue if possible.
            for (int leaves = 1; leaves < maxLeafs; leaves++) {
                // parent is the leaf to split
                TrainNode parent = nextSplits.poll();
                if (parent == null) {
                    break;
                }
                parent.split(nextSplits); // Split the parent node into two children nodes
            }
        }
    }

    private static void checkArgument(@Nonnull double[][] x, @Nonnull int[] y, int numVars,
            int maxDepth, int maxLeafs, int minSplits, int minLeafSize) {
        if (x.length != y.length) {
            throw new IllegalArgumentException(String.format(
                "The sizes of X and Y don't match: %d != %d", x.length, y.length));
        }
        if (numVars <= 0 || numVars > x[0].length) {
            throw new IllegalArgumentException(
                "Invalid number of variables to split on at a node of the tree: " + numVars);
        }
        if (maxDepth < 2) {
            throw new IllegalArgumentException("maxDepth should be greater than 1: " + maxDepth);
        }
        if (maxLeafs < 2) {
            throw new IllegalArgumentException("Invalid maximum leaves: " + maxLeafs);
        }
        if (minSplits < 2) {
            throw new IllegalArgumentException(
                "Invalid minimum number of samples required to split an internal node: "
                        + minSplits);
        }
        if (minLeafSize < 1) {
            throw new IllegalArgumentException("Invalid minimum size of leaf nodes: " + minLeafSize);
        }
    }

    /**
     * Returns the variable importance. Every time a split of a node is made on variable the (GINI,
     * information gain, etc.) impurity criterion for the two descendent nodes is less than the
     * parent node. Adding up the decreases for each individual variable over the tree gives a
     * simple measure of variable importance.
     *
     * @return the variable importance
     */
    public double[] importance() {
        return _importance;
    }

    @Override
    public int predict(final double[] x) {
        return _root.predict(x);
    }

    /**
     * Predicts the class label of an instance and also calculate a posteriori probabilities. Not
     * supported.
     */
    @Override
    public int predict(double[] x, double[] posteriori) {
        throw new UnsupportedOperationException("Not supported.");
    }

    public String predictJsCodegen() {
        StringBuilder buf = new StringBuilder(1024);
        _root.jsCodegen(buf, 0);
        return buf.toString();
    }

    public String predictOpCodegen(String sep) {
        List<String> opslist = new ArrayList<String>();
        _root.opCodegen(opslist, 0);
        opslist.add("call end");
        String scripts = StringUtils.concat(opslist, sep);
        return scripts;
    }

    @Nonnull
    public byte[] predictSerCodegen(boolean compress) throws HiveException {
        try {
            if (compress) {
                return ObjectUtils.toCompressedBytes(_root);
            } else {
                return ObjectUtils.toBytes(_root);
            }
        } catch (IOException ioe) {
            throw new HiveException("IOException cause while serializing DecisionTree object", ioe);
        } catch (Exception e) {
            throw new HiveException("Exception cause while serializing DecisionTree object", e);
        }
    }

    public static Node deserializeNode(final byte[] serializedObj, final int length,
            final boolean compressed) throws HiveException {
        final Node root = new Node();
        try {
            if (compressed) {
                ObjectUtils.readCompressedObject(serializedObj, 0, length, root);
            } else {
                ObjectUtils.readObject(serializedObj, length, root);
            }
        } catch (IOException ioe) {
            throw new HiveException("IOException cause while deserializing DecisionTree object",
                ioe);
        } catch (Exception e) {
            throw new HiveException("Exception cause while deserializing DecisionTree object", e);
        }
        return root;
    }

    @Override
    public String toString() {
        return _root == null ? "" : predictJsCodegen();
    }

}
