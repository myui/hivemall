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

import hivemall.smile.utils.SmileExtUtils;
import hivemall.utils.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import smile.classification.Classifier;
import smile.data.Attribute;
import smile.data.NominalAttribute;
import smile.math.Math;
import smile.math.Random;

/**
 * Decision tree for classification. A decision tree can be learned by splitting
 * the training set into subsets based on an attribute value test. This process
 * is repeated on each derived subset in a recursive manner called recursive
 * partitioning. The recursion is completed when the subset at a node all has
 * the same value of the target variable, or when splitting no longer adds value
 * to the predictions.
 * <p>
 * The algorithms that are used for constructing decision trees usually work
 * top-down by choosing a variable at each step that is the next best variable
 * to use in splitting the set of items. "Best" is defined by how well the
 * variable splits the set into homogeneous subsets that have the same value of
 * the target variable. Different algorithms use different formulae for
 * measuring "best". Used by the CART algorithm, Gini impurity is a measure of
 * how often a randomly chosen element from the set would be incorrectly labeled
 * if it were randomly labeled according to the distribution of labels in the
 * subset. Gini impurity can be computed by summing the probability of each item
 * being chosen times the probability of a mistake in categorizing that item. It
 * reaches its minimum (zero) when all cases in the node fall into a single
 * target category. Information gain is another popular measure, used by the
 * ID3, C4.5 and C5.0 algorithms. Information gain is based on the concept of
 * entropy used in information theory. For categorical variables with different
 * number of levels, however, information gain are biased in favor of those
 * attributes with more levels. Instead, one may employ the information gain
 * ratio, which solves the drawback of information gain.
 * <p>
 * Classification and Regression Tree techniques have a number of advantages
 * over many of those alternative techniques.
 * <dl>
 * <dt>Simple to understand and interpret.</dt>
 * <dd>In most cases, the interpretation of results summarized in a tree is very
 * simple. This simplicity is useful not only for purposes of rapid
 * classification of new observations, but can also often yield a much simpler
 * "model" for explaining why observations are classified or predicted in a
 * particular manner.</dd>
 * <dt>Able to handle both numerical and categorical data.</dt>
 * <dd>Other techniques are usually specialized in analyzing datasets that have
 * only one type of variable.</dd>
 * <dt>Tree methods are nonparametric and nonlinear.</dt>
 * <dd>The final results of using tree methods for classification or regression
 * can be summarized in a series of (usually few) logical if-then conditions
 * (tree nodes). Therefore, there is no implicit assumption that the underlying
 * relationships between the predictor variables and the dependent variable are
 * linear, follow some specific non-linear link function, or that they are even
 * monotonic in nature. Thus, tree methods are particularly well suited for data
 * mining tasks, where there is often little a priori knowledge nor any coherent
 * set of theories or predictions regarding which variables are related and how.
 * In those types of data analytics, tree methods can often reveal simple
 * relationships between just a few variables that could have easily gone
 * unnoticed using other analytic techniques.</dd>
 * </dl>
 * One major problem with classification and regression trees is their high
 * variance. Often a small change in the data can result in a very different
 * series of splits, making interpretation somewhat precarious. Besides,
 * decision-tree learners can create over-complex trees that cause over-fitting.
 * Mechanisms such as pruning are necessary to avoid this problem. Another
 * limitation of trees is the lack of smoothness of the prediction surface.
 * <p>
 * Some techniques such as bagging, boosting, and random forest use more than
 * one decision tree for their analysis.
 * 
 * @author Haifeng Li
 * @author Makoto Yui
 */
public class DecisionTree implements Classifier<double[]> {
    /**
     * The attributes of independent variable.
     */
    private final Attribute[] _attributes;
    /**
     * Variable importance. Every time a split of a node is made on variable the
     * (GINI, information gain, etc.) impurity criterion for the two descendent
     * nodes is less than the parent node. Adding up the decreases for each
     * individual variable over the tree gives a simple measure of variable
     * importance.
     */
    private final double[] _importance;
    /**
     * The root of the regression tree
     */
    private final Node _root;
    /**
     * The splitting rule.
     */
    private final SplitRule _rule;
    /**
     * The number of classes.
     */
    private final int _k;
    /**
     * The number of input variables to be used to determine the decision at a
     * node of the tree.
     */
    private final int _numVars;
    /**
     * The number of instances in a node below which the tree will not split.
     */
    private final int _minSplit;
    /**
     * The index of training values in ascending order. Note that only numeric
     * attributes will be sorted.
     */
    private final int[][] _order;

    private final Random _rnd;

    /**
     * The criterion to choose variable to split instances.
     */
    public static enum SplitRule {
        /**
         * Used by the CART algorithm, Gini impurity is a measure of how often a
         * randomly chosen element from the set would be incorrectly labeled if
         * it were randomly labeled according to the distribution of labels in
         * the subset. Gini impurity can be computed by summing the probability
         * of each item being chosen times the probability of a mistake in
         * categorizing that item. It reaches its minimum (zero) when all cases
         * in the node fall into a single target category.
         */
        GINI,
        /**
         * Used by the ID3, C4.5 and C5.0 tree generation algorithms.
         */
        ENTROPY
    }

    /**
     * Classification tree node.
     */
    class Node {

        /**
         * Predicted class label for this node.
         */
        int output = -1;
        /**
         * The split feature for this node.
         */
        int splitFeature = -1;
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

        /**
         * Constructor.
         */
        public Node() {}

        /**
         * Constructor.
         */
        public Node(int output) {
            this.output = output;
        }

        /**
         * Evaluate the regression tree over an instance.
         */
        public int predict(double[] x) {
            if(trueChild == null && falseChild == null) {
                return output;
            } else {
                if(_attributes[splitFeature].type == Attribute.Type.NOMINAL) {
                    if(x[splitFeature] == splitValue) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                } else if(_attributes[splitFeature].type == Attribute.Type.NUMERIC) {
                    if(x[splitFeature] <= splitValue) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                } else {
                    throw new IllegalStateException("Unsupported attribute type: "
                            + _attributes[splitFeature].type);
                }
            }
        }

        public void codegen(@Nonnull final StringBuilder builder, final int depth) {
            if(trueChild == null && falseChild == null) {
                indent(builder, depth);
                builder.append("").append(output).append(";\n");
            } else {
                if(_attributes[splitFeature].type == Attribute.Type.NOMINAL) {
                    indent(builder, depth);
                    builder.append("if(x[").append(splitFeature).append("] == ").append(splitValue).append(") {\n");
                    trueChild.codegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("} else {\n");
                    falseChild.codegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("}\n");
                } else if(_attributes[splitFeature].type == Attribute.Type.NUMERIC) {
                    indent(builder, depth);
                    builder.append("if(x[").append(splitFeature).append("] <= ").append(splitValue).append(") {\n");
                    trueChild.codegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("} else  {\n");
                    falseChild.codegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("}\n");
                } else {
                    throw new IllegalStateException("Unsupported attribute type: "
                            + _attributes[splitFeature].type);
                }
            }
        }

        public int opcodegen(final List<String> scripts, int depth) {
            int selfDepth = 0;
            final StringBuilder buf = new StringBuilder();
            if(trueChild == null && falseChild == null) {
                buf.append("push ").append(output);
                scripts.add(buf.toString());
                buf.setLength(0);
                buf.append("goto last");
                scripts.add(buf.toString());
                selfDepth += 2;
            } else {
                if(_attributes[splitFeature].type == Attribute.Type.NOMINAL) {
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
                    int trueDepth = trueChild.opcodegen(scripts, depth);
                    selfDepth += trueDepth;
                    scripts.set(depth - 1, "ifeq " + String.valueOf(depth + trueDepth));
                    int falseDepth = falseChild.opcodegen(scripts, depth + trueDepth);
                    selfDepth += falseDepth;
                } else if(_attributes[splitFeature].type == Attribute.Type.NUMERIC) {
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
                    int trueDepth = trueChild.opcodegen(scripts, depth);
                    selfDepth += trueDepth;
                    scripts.set(depth - 1, "ifle " + String.valueOf(depth + trueDepth));
                    int falseDepth = falseChild.opcodegen(scripts, depth + trueDepth);
                    selfDepth += falseDepth;
                } else {
                    throw new IllegalStateException("Unsupported attribute type: "
                            + _attributes[splitFeature].type);
                }
            }
            return selfDepth;
        }
    }

    private static void indent(final StringBuilder builder, final int depth) {
        for(int i = 0; i < depth; i++) {
            builder.append("  ");
        }
    }

    /**
     * Classification tree node for training purpose.
     */
    class TrainNode implements Comparable<TrainNode> {
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
        /**
         * The samples for training this node. Note that samples[i] is the
         * number of sampling of dataset[i]. 0 means that the datum is not
         * included and values of greater than 1 are possible because of
         * sampling with replacement.
         */
        final int[] samples;

        /**
         * Constructor.
         */
        public TrainNode(Node node, double[][] x, int[] y, int[] samples) {
            this.node = node;
            this.x = x;
            this.y = y;
            this.samples = samples;
        }

        @Override
        public int compareTo(TrainNode a) {
            return (int) Math.signum(a.node.splitScore - node.splitScore);
        }

        /**
         * Finds the best attribute to split on at the current node. Returns
         * true if a split exists to reduce squared error, false otherwise.
         */
        public boolean findBestSplit() {
            int N = x.length;
            int label = -1;
            boolean pure = true;
            for(int i = 0; i < N; i++) {
                if(samples[i] > 0) {
                    if(label == -1) {
                        label = y[i];
                    } else if(y[i] != label) {
                        pure = false;
                        break;
                    }
                }
            }

            // Since all instances have same label, stop splitting.
            if(pure) {
                return false;
            }

            // Sample count in each class.
            int n = 0;
            int[] count = new int[_k];
            int[] falseCount = new int[_k];
            for(int i = 0; i < N; i++) {
                if(samples[i] > 0) {
                    n += samples[i];
                    count[y[i]] += samples[i];
                }
            }

            double impurity = impurity(count, n);

            int p = _attributes.length;
            int[] variables = new int[p];
            for(int i = 0; i < p; i++) {
                variables[i] = i;
            }

            if(_numVars < p) {
                SmileExtUtils.shuffle(variables, _rnd);
            }
            for(int j = 0; j < _numVars; j++) {
                Node split = findBestSplit(n, count, falseCount, impurity, variables[j]);
                if(split.splitScore > node.splitScore) {
                    node.splitFeature = split.splitFeature;
                    node.splitValue = split.splitValue;
                    node.splitScore = split.splitScore;
                    node.trueChildOutput = split.trueChildOutput;
                    node.falseChildOutput = split.falseChildOutput;
                }
            }

            return (node.splitFeature != -1);
        }

        /**
         * Finds the best split cutoff for attribute j at the current node.
         * 
         * @param n
         *            the number instances in this node.
         * @param count
         *            the sample count in each class.
         * @param falseCount
         *            an array to store sample count in each class for false
         *            child node.
         * @param impurity
         *            the impurity of this node.
         * @param j
         *            the attribute to split on.
         */
        public Node findBestSplit(int n, int[] count, int[] falseCount, double impurity, int j) {
            int N = x.length;
            Node splitNode = new Node();

            if(_attributes[j].type == Attribute.Type.NOMINAL) {
                int m = ((NominalAttribute) _attributes[j]).size();
                int[][] trueCount = new int[m][_k];

                for(int i = 0; i < N; i++) {
                    if(samples[i] > 0) {
                        trueCount[(int) x[i][j]][y[i]] += samples[i];
                    }
                }

                for(int l = 0; l < m; l++) {
                    int tc = Math.sum(trueCount[l]);
                    int fc = n - tc;

                    // If either side is empty, skip this feature.
                    if(tc == 0 || fc == 0) {
                        continue;
                    }

                    for(int q = 0; q < _k; q++) {
                        falseCount[q] = count[q] - trueCount[l][q];
                    }

                    int trueLabel = Math.whichMax(trueCount[l]);
                    int falseLabel = Math.whichMax(falseCount);
                    double gain = impurity - (double) tc / n * impurity(trueCount[l], tc)
                            - (double) fc / n * impurity(falseCount, fc);

                    if(gain > splitNode.splitScore) {
                        // new best split
                        splitNode.splitFeature = j;
                        splitNode.splitValue = l;
                        splitNode.splitScore = gain;
                        splitNode.trueChildOutput = trueLabel;
                        splitNode.falseChildOutput = falseLabel;
                    }
                }
            } else if(_attributes[j].type == Attribute.Type.NUMERIC) {
                int[] trueCount = new int[_k];
                double prevx = Double.NaN;
                int prevy = -1;

                for(int i : _order[j]) {
                    if(samples[i] > 0) {
                        if(Double.isNaN(prevx) || x[i][j] == prevx || y[i] == prevy) {
                            prevx = x[i][j];
                            prevy = y[i];
                            trueCount[y[i]] += samples[i];
                            continue;
                        }

                        int tc = Math.sum(trueCount);
                        int fc = n - tc;

                        // If either side is empty, continue.
                        if(tc == 0 || fc == 0) {
                            prevx = x[i][j];
                            prevy = y[i];
                            trueCount[y[i]] += samples[i];
                            continue;
                        }

                        for(int l = 0; l < _k; l++) {
                            falseCount[l] = count[l] - trueCount[l];
                        }

                        int trueLabel = Math.whichMax(trueCount);
                        int falseLabel = Math.whichMax(falseCount);
                        double gain = impurity - (double) tc / n * impurity(trueCount, tc)
                                - (double) fc / n * impurity(falseCount, fc);

                        if(gain > splitNode.splitScore) {
                            // new best split
                            splitNode.splitFeature = j;
                            splitNode.splitValue = (x[i][j] + prevx) / 2;
                            splitNode.splitScore = gain;
                            splitNode.trueChildOutput = trueLabel;
                            splitNode.falseChildOutput = falseLabel;
                        }

                        prevx = x[i][j];
                        prevy = y[i];
                        trueCount[y[i]] += samples[i];
                    }
                }
            } else {
                throw new IllegalStateException("Unsupported attribute type: "
                        + _attributes[j].type);
            }

            return splitNode;
        }

        /**
         * Split the node into two children nodes. Returns true if split
         * success.
         */
        public boolean split(PriorityQueue<TrainNode> nextSplits) {
            if(node.splitFeature < 0) {
                throw new IllegalStateException("Split a node with invalid feature.");
            }

            int n = x.length;
            int tc = 0;
            int fc = 0;
            int[] trueSamples = new int[n];
            int[] falseSamples = new int[n];

            if(_attributes[node.splitFeature].type == Attribute.Type.NOMINAL) {
                for(int i = 0; i < n; i++) {
                    if(samples[i] > 0) {
                        if(x[i][node.splitFeature] == node.splitValue) {
                            trueSamples[i] = samples[i];
                            tc += samples[i];
                        } else {
                            falseSamples[i] = samples[i];
                            fc += samples[i];
                        }
                    }
                }
            } else if(_attributes[node.splitFeature].type == Attribute.Type.NUMERIC) {
                for(int i = 0; i < n; i++) {
                    if(samples[i] > 0) {
                        if(x[i][node.splitFeature] <= node.splitValue) {
                            trueSamples[i] = samples[i];
                            tc += samples[i];
                        } else {
                            falseSamples[i] = samples[i];
                            fc += samples[i];
                        }
                    }
                }
            } else {
                throw new IllegalStateException("Unsupported attribute type: "
                        + _attributes[node.splitFeature].type);
            }

            if(tc == 0 || fc == 0) {
                node.splitFeature = -1;
                node.splitValue = Double.NaN;
                node.splitScore = 0.0;
                return false;
            }

            node.trueChild = new Node(node.trueChildOutput);
            node.falseChild = new Node(node.falseChildOutput);

            TrainNode trueChild = new TrainNode(node.trueChild, x, y, trueSamples);
            if(tc >= _minSplit && trueChild.findBestSplit()) {
                if(nextSplits != null) {
                    nextSplits.add(trueChild);
                } else {
                    trueChild.split(null);
                }
            }

            TrainNode falseChild = new TrainNode(node.falseChild, x, y, falseSamples);
            if(fc >= _minSplit && falseChild.findBestSplit()) {
                if(nextSplits != null) {
                    nextSplits.add(falseChild);
                } else {
                    falseChild.split(null);
                }
            }

            _importance[node.splitFeature] += node.splitScore;

            return true;
        }
    }

    /**
     * Returns the impurity of a node.
     * 
     * @param count
     *            the sample count in each class.
     * @param n
     *            the number of samples in the node.
     * @return the impurity of a node
     */
    private double impurity(int[] count, int n) {
        double impurity = 0.0;

        switch (_rule) {
            case GINI: {
                impurity = 1.0;
                for(int i = 0; i < count.length; i++) {
                    if(count[i] > 0) {
                        double p = (double) count[i] / n;
                        impurity -= p * p;
                    }
                }
                break;
            }
            case ENTROPY: {
                for(int i = 0; i < count.length; i++) {
                    if(count[i] > 0) {
                        double p = (double) count[i] / n;
                        impurity -= p * Math.log2(p);
                    }
                }
                break;
            }
        }

        return impurity;
    }

    /**
     * @see DecisionTree#DecisionTree(Attribute[], double[][], int[], int, int, int, int[], int[][], SplitRule, Random)
     */
    public DecisionTree(@Nonnull double[][] x, @Nonnull int[] y, int J, @Nonnull SplitRule rule) {
        this(null, x, y, x[0].length, J, 2, null, null, rule, null);
    }

    /**
     * @see DecisionTree#DecisionTree(Attribute[], double[][], int[], int, int, int, int[], int[][], SplitRule, Random)
     */
    public DecisionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull int[] y, int J) {
        this(attributes, x, y, x[0].length, J, 2, null, null, SplitRule.GINI, null);
    }

    /**
     * @see DecisionTree#DecisionTree(Attribute[], double[][], int[], int, int, int, int[], int[][], SplitRule, Random)
     */
    public DecisionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull int[] y, int J, @Nonnull SplitRule rule) {
        this(attributes, x, y, x[0].length, J, 2, null, null, rule, null);
    }

    /**
     * Constructor. Learns a classification tree for random forest.
     *
     * @param attributes
     *            the attribute properties.
     * @param x
     *            the training instances.
     * @param y
     *            the response variable.
     * @param numVars
     *            the number of input variables to pick to split on at each
     *            node. It seems that dim/3 give generally good performance,
     *            where dim is the number of variables.
     * @param numLeafs
     *            the maximum number of leaf nodes in the tree.
     * @param minSplits
     *            the number of minimum elements in a node to split
     * @param order
     *            the index of training values in ascending order. Note that
     *            only numeric attributes need be sorted.
     * @param samples
     *            the sample set of instances for stochastic learning.
     *            samples[i] is the number of sampling for instance i.
     * @param rule
     *            the splitting rule.
     * @param seed 
     */
    public DecisionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull int[] y, int numVars, int numLeafs, int minSplits, @Nullable int[] samples, @Nullable int[][] order, @Nonnull SplitRule rule, @Nullable smile.math.Random rand) {
        if(x.length != y.length) {
            throw new IllegalArgumentException(String.format("The sizes of X and Y don't match: %d != %d", x.length, y.length));
        }
        if(numVars <= 0 || numVars > x[0].length) {
            throw new IllegalArgumentException("Invalid number of variables to split on at a node of the tree: "
                    + numVars);
        }
        if(numLeafs < 2) {
            throw new IllegalArgumentException("Invalid maximum leaves: " + numLeafs);
        }

        this._k = Math.max(y) + 1;
        if(_k < 2) {
            throw new IllegalArgumentException("Only one class or negative class labels.");
        }

        this._attributes = SmileExtUtils.attributeTypes(attributes, x);
        if(attributes.length != x[0].length) {
            throw new IllegalArgumentException("-attrs option is invliad: "
                    + Arrays.toString(attributes));
        }
        this._numVars = numVars;
        this._minSplit = minSplits;
        this._rule = rule;
        this._order = (order == null) ? SmileExtUtils.sort(attributes, x) : order;
        this._importance = new double[attributes.length];
        this._rnd = (rand == null) ? new smile.math.Random() : rand;

        int n = y.length;
        int[] count = new int[_k];
        if(samples == null) {
            samples = new int[n];
            for(int i = 0; i < n; i++) {
                samples[i] = 1;
                count[y[i]]++;
            }
        } else {
            for(int i = 0; i < n; i++) {
                count[y[i]] += samples[i];
            }
        }

        this._root = new Node(Math.whichMax(count));

        TrainNode trainRoot = new TrainNode(_root, x, y, samples);
        if(numLeafs == Integer.MAX_VALUE) {
            if(trainRoot.findBestSplit()) {
                trainRoot.split(null);
            }
        } else {
            // Priority queue for best-first tree growing.
            PriorityQueue<TrainNode> nextSplits = new PriorityQueue<TrainNode>();
            // Now add splits to the tree until max tree size is reached
            if(trainRoot.findBestSplit()) {
                nextSplits.add(trainRoot);
            }
            // Pop best leaf from priority queue, split it, and push
            // children nodes into the queue if possible.
            for(int leaves = 1; leaves < numLeafs; leaves++) {
                // parent is the leaf to split
                TrainNode node = nextSplits.poll();
                if(node == null) {
                    break;
                }
                node.split(nextSplits); // Split the parent node into two children nodes
            }
        }
    }

    /**
     * Returns the variable importance. Every time a split of a node is made on
     * variable the (GINI, information gain, etc.) impurity criterion for the
     * two descendent nodes is less than the parent node. Adding up the
     * decreases for each individual variable over the tree gives a simple
     * measure of variable importance.
     *
     * @return the variable importance
     */
    public double[] importance() {
        return _importance;
    }

    @Override
    public int predict(double[] x) {
        return _root.predict(x);
    }

    /**
     * Predicts the class label of an instance and also calculate a posteriori
     * probabilities. Not supported.
     */
    @Override
    public int predict(double[] x, double[] posteriori) {
        throw new UnsupportedOperationException("Not supported.");
    }

    public String predictCodegen() {
        StringBuilder buf = new StringBuilder(1024);
        _root.codegen(buf, 0);
        return buf.toString();
    }

    public String predictOpCodegen(String sep) {
        List<String> opslist = new ArrayList<String>();
        _root.opcodegen(opslist, 0);
        opslist.add("call end");
        String scripts = StringUtils.concat(opslist, sep);
        return scripts;
    }

}
