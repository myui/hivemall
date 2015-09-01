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
package hivemall.smile.regression;

import hivemall.smile.utils.SmileExtUtils;
import hivemall.utils.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import smile.data.Attribute;
import smile.data.NominalAttribute;
import smile.math.Math;
import smile.math.Random;
import smile.regression.GradientTreeBoost;
import smile.regression.RandomForest;
import smile.regression.Regression;

/**
 * Decision tree for regression. A decision tree can be learned by splitting the
 * training set into subsets based on an attribute value test. This process is
 * repeated on each derived subset in a recursive manner called recursive
 * partitioning.
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
 * @see GradientTreeBoost
 * @see RandomForest
 * 
 * @author Haifeng LI
 */
public class RegressionTree implements Regression<double[]> {
    /**
     * The attributes of independent variable.
     */
    private final Attribute[] attributes;
    /**
     * Variable importance. Every time a split of a node is made on variable the
     * impurity criterion for the two descendent nodes is less than the parent
     * node. Adding up the decreases for each individual variable over the tree
     * gives a simple measure of variable importance.
     */
    private final double[] importance;
    /**
     * The root of the regression tree
     */
    private final Node root;
    /**
     * The number of instances in a node below which the tree will not split,
     * setting S = 5 generally gives good results.
     */
    private final int S;
    /**
     * The maximum number of leaf nodes in the tree.
     */
    private final int J;
    /**
     * The number of input variables to be used to determine the decision at a
     * node of the tree.
     */
    private final int M;
    /**
     * The index of training values in ascending order. Note that only numeric
     * attributes will be sorted.
     */
    private final int[][] order;
    
    private final Random rnd;

    /**
     * An interface to calculate node output. Note that samples[i] is the number
     * of sampling of dataset[i]. 0 means that the datum is not included and
     * values of greater than 1 are possible because of sampling with
     * replacement.
     */
    public static interface NodeOutput {
        /**
         * Calculate the node output.
         * 
         * @param samples
         *            the samples in the node.
         * @return the node output
         */
        public double calculate(int[] samples);
    }

    /**
     * Regression tree node.
     */
    class Node {

        /**
         * Predicted real value for this node.
         */
        double output = 0.0;
        /**
         * The split feature for this node.
         */
        int splitFeature = -1;
        /**
         * The split value.
         */
        double splitValue = Double.NaN;
        /**
         * Reduction in squared error compared to parent.
         */
        double splitScore = 0.0;
        /**
         * Children node.
         */
        Node trueChild;
        /**
         * Children node.
         */
        Node falseChild;
        /**
         * Predicted output for children node.
         */
        double trueChildOutput = 0.0;
        /**
         * Predicted output for children node.
         */
        double falseChildOutput = 0.0;

        /**
         * Constructor.
         */
        public Node(double output) {
            this.output = output;
        }

        /**
         * Evaluate the regression tree over an instance.
         */
        public double predict(double[] x) {
            if(trueChild == null && falseChild == null) {
                return output;
            } else {
                if(attributes[splitFeature].type == Attribute.Type.NOMINAL) {
                    if(Math.equals(x[splitFeature], splitValue)) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                } else if(attributes[splitFeature].type == Attribute.Type.NUMERIC) {
                    if(x[splitFeature] <= splitValue) {
                        return trueChild.predict(x);
                    } else {
                        return falseChild.predict(x);
                    }
                } else {
                    throw new IllegalStateException("Unsupported attribute type: "
                            + attributes[splitFeature].type);
                }
            }
        }

        /**
         * Evaluate the regression tree over an instance.
         */
        public double predict(int[] x) {
            if(trueChild == null && falseChild == null) {
                return output;
            } else if(x[splitFeature] == (int) splitValue) {
                return trueChild.predict(x);
            } else {
                return falseChild.predict(x);
            }
        }

        public void codegen(@Nonnull final StringBuilder builder, final int depth) {
            if(trueChild == null && falseChild == null) {
                indent(builder, depth);
                builder.append("").append(output).append(";\n");
            } else {
                if(attributes[splitFeature].type == Attribute.Type.NOMINAL) {
                    indent(builder, depth);
                    builder.append("if(x[").append(splitFeature).append("] == ").append(splitValue).append(") {\n");
                    trueChild.codegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("} else {\n");
                    falseChild.codegen(builder, depth + 1);
                    indent(builder, depth);
                    builder.append("}\n");
                } else if(attributes[splitFeature].type == Attribute.Type.NUMERIC) {
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
                            + attributes[splitFeature].type);
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
                if(attributes[splitFeature].type == Attribute.Type.NOMINAL) {
                    buf.append("push ").append("x[").append(splitFeature).append("]");
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("push ").append(splitValue);
                    scripts.add(buf.toString());
                    buf.setLength(0);
                    buf.append("ifne ");
                    scripts.add(buf.toString());
                    depth += 3;
                    selfDepth += 3;
                    int trueDepth = trueChild.opcodegen(scripts, depth);
                    selfDepth += trueDepth;
                    scripts.set(depth - 1, "ifne " + String.valueOf(depth + trueDepth));
                    int falseDepth = falseChild.opcodegen(scripts, depth + trueDepth);
                    selfDepth += falseDepth;
                } else if(attributes[splitFeature].type == Attribute.Type.NUMERIC) {
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
                            + attributes[splitFeature].type);
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
     * Regression tree node for training purpose.
     */
    class TrainNode implements Comparable<TrainNode> {
        /**
         * The associated regression tree node.
         */
        Node node;
        /**
         * Child node that passes the test.
         */
        TrainNode trueChild;
        /**
         * Child node that fails the test.
         */
        TrainNode falseChild;
        /**
         * Training dataset.
         */
        double[][] x;
        /**
         * Training data response value.
         */
        double[] y;
        /**
         * The samples for training this node. Note that samples[i] is the
         * number of sampling of dataset[i]. 0 means that the datum is not
         * included and values of greater than 1 are possible because of
         * sampling with replacement.
         */
        int[] samples;

        /**
         * Constructor.
         */
        public TrainNode(Node node, double[][] x, double[] y, int[] samples) {
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
         * Calculate the node output for leaves.
         * 
         * @param output
         *            the output calculate functor.
         */
        public void calculateOutput(NodeOutput output) {
            if(node.trueChild == null && node.falseChild == null) {
                node.output = output.calculate(samples);
            } else {
                if(trueChild != null) {
                    trueChild.calculateOutput(output);
                }
                if(falseChild != null) {
                    falseChild.calculateOutput(output);
                }
            }
        }

        /**
         * Finds the best attribute to split on at the current node. Returns
         * true if a split exists to reduce squared error, false otherwise.
         */
        public boolean findBestSplit() {
            int n = 0;
            for(int s : samples) {
                n += s;
            }

            if(n <= S) {
                return false;
            }

            double sum = node.output * n;
            int p = attributes.length;
            int[] variables = new int[p];
            for(int i = 0; i < p; i++) {
                variables[i] = i;
            }

            // Loop through features and compute the reduction of squared error,
            // which is trueCount * trueMean^2 + falseCount * falseMean^2 - count * parentMean^2                    
            if(M < p) {
                // Training of Random Forest will get into this race condition.
                // smile.math.Math uses a static object of random number generator.
                SmileExtUtils.permutate(variables, rnd);
            }
            for(int j = 0; j < M; j++) {
                Node split = findBestSplit(n, sum, variables[j]);
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
         * @param impurity
         *            the impurity of this node.
         * @param j
         *            the attribute to split on.
         */
        public Node findBestSplit(int n, double sum, int j) {
            int N = x.length;
            Node split = new Node(0.0);
            if(attributes[j].type == Attribute.Type.NOMINAL) {
                int m = ((NominalAttribute) attributes[j]).size();
                double[] trueSum = new double[m];
                int[] trueCount = new int[m];

                for(int i = 0; i < N; i++) {
                    if(samples[i] > 0) {
                        double target = samples[i] * y[i];

                        // For each true feature of this datum increment the
                        // sufficient statistics for the "true" branch to evaluate
                        // splitting on this feature.
                        int index = (int) x[i][j];
                        trueSum[index] += target;
                        trueCount[index] += samples[i];
                    }
                }

                for(int k = 0; k < m; k++) {
                    double tc = (double) trueCount[k];
                    double fc = n - tc;

                    // If either side is empty, skip this feature.
                    if(tc == 0 || fc == 0) {
                        continue;
                    }

                    // compute penalized means
                    double trueMean = trueSum[k] / tc;
                    double falseMean = (sum - trueSum[k]) / fc;

                    double gain = (tc * trueMean * trueMean + fc * falseMean * falseMean) - n
                            * split.output * split.output;
                    if(gain > split.splitScore) {
                        // new best split
                        split.splitFeature = j;
                        split.splitValue = k;
                        split.splitScore = gain;
                        split.trueChildOutput = trueMean;
                        split.falseChildOutput = falseMean;
                    }
                }
            } else if(attributes[j].type == Attribute.Type.NUMERIC) {
                double trueSum = 0.0;
                int trueCount = 0;
                double prevx = Double.NaN;

                for(int i : order[j]) {
                    if(samples[i] > 0) {
                        if(Double.isNaN(prevx) || x[i][j] == prevx) {
                            prevx = x[i][j];
                            trueSum += samples[i] * y[i];
                            trueCount += samples[i];
                            continue;
                        }

                        double falseCount = n - trueCount;

                        // If either side is empty, skip this feature.
                        if(trueCount == 0 || falseCount == 0) {
                            prevx = x[i][j];
                            trueSum += samples[i] * y[i];
                            trueCount += samples[i];
                            continue;
                        }

                        // compute penalized means
                        double trueMean = trueSum / trueCount;
                        double falseMean = (sum - trueSum) / falseCount;

                        // The gain is actually -(reduction in squared error) for
                        // sorting in priority queue, which treats smaller number with
                        // higher priority.
                        double gain = (trueCount * trueMean * trueMean + falseCount * falseMean
                                * falseMean)
                                - n * split.output * split.output;
                        if(gain > split.splitScore) {
                            // new best split
                            split.splitFeature = j;
                            split.splitValue = (x[i][j] + prevx) / 2;
                            split.splitScore = gain;
                            split.trueChildOutput = trueMean;
                            split.falseChildOutput = falseMean;
                        }

                        prevx = x[i][j];
                        trueSum += samples[i] * y[i];
                        trueCount += samples[i];
                    }
                }
            } else {
                throw new IllegalStateException("Unsupported attribute type: " + attributes[j].type);
            }

            return split;
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

            if(attributes[node.splitFeature].type == Attribute.Type.NOMINAL) {
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
            } else if(attributes[node.splitFeature].type == Attribute.Type.NUMERIC) {
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
                        + attributes[node.splitFeature].type);
            }

            if(tc == 0 || fc == 0) {
                node.splitFeature = -1;
                node.splitValue = Double.NaN;
                node.splitScore = 0.0;
                return false;
            }

            node.trueChild = new Node(node.trueChildOutput);
            node.falseChild = new Node(node.falseChildOutput);

            trueChild = new TrainNode(node.trueChild, x, y, trueSamples);
            if(tc > S && trueChild.findBestSplit()) {
                if(nextSplits != null) {
                    nextSplits.add(trueChild);
                } else {
                    trueChild.split(null);
                }
            }

            falseChild = new TrainNode(node.falseChild, x, y, falseSamples);
            if(fc > S && falseChild.findBestSplit()) {
                if(nextSplits != null) {
                    nextSplits.add(falseChild);
                } else {
                    falseChild.split(null);
                }
            }

            importance[node.splitFeature] += node.splitScore;

            return true;
        }
    }

    public RegressionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull double[] y, int J) {
        this(attributes, x, y, x[0].length, 5, J, null, null, SmileExtUtils.generateSeed());
    }

    /**
     * @see RegressionTree#RegressionTree(Attribute[], double[][], double[], int, int, int, int[][], int[], NodeOutput, seeds)
     */
    public RegressionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull double[] y, int M, int S, int J, @Nullable int[][] order, @Nullable int[] samples, long seed) {
        this(attributes, x, y, M, S, J, order, samples, null, seed);
    }

    /**
     * Constructor. Learns a regression tree for gradient tree boosting.
     * 
     * @param attributes
     *            the attribute properties.
     * @param x
     *            the training instances.
     * @param y
     *            the response variable.
     * @param M
     *            the number of input variables to pick to split on at each
     *            node. It seems that dim/3 give generally good performance,
     *            where dim is the number of variables.
     * @param S
     *            number of instances in a node below which the tree will not
     *            split, setting S = 5 generally gives good results.
     * @param J
     *            the maximum number of leaf nodes in the tree.
     * @param order
     *            the index of training values in ascending order. Note that
     *            only numeric attributes need be sorted.
     * @param samples
     *            the sample set of instances for stochastic learning.
     *            samples[i] should be 0 or 1 to indicate if the instance is
     *            used for training.
     * @param output
     *            An interface to calculate node output.
     */
    public RegressionTree(@Nullable Attribute[] attributes, @Nonnull double[][] x, @Nonnull double[] y, int M, int S, int J, @Nullable int[][] order, @Nullable int[] samples, @Nullable NodeOutput output, long seed) {
        if(x.length != y.length) {
            throw new IllegalArgumentException(String.format("The sizes of X and Y don't match: %d != %d", x.length, y.length));
        }
        if(M <= 0 || M > x[0].length) {
            throw new IllegalArgumentException("Invalid number of variables to split on at a node of the tree: "
                    + M);
        }
        if(S <= 0) {
            throw new IllegalArgumentException("Invalid mimum number of instances in leaf nodes: "
                    + S);
        }
        if(J < 2) {
            throw new IllegalArgumentException("Invalid maximum leaves: " + J);
        }

        this.attributes = SmileExtUtils.attributeTypes(attributes, x);
        if(attributes.length != x[0].length) {
            throw new IllegalArgumentException("-attrs option is invliad: "
                    + Arrays.toString(attributes));
        }

        this.M = M;
        this.S = S;
        this.J = J;
        this.order = (order == null) ? SmileExtUtils.sort(attributes, x) : order;
        this.importance = new double[attributes.length];
        this.rnd = new Random(seed);

        int n = 0;
        double sum = 0.0;
        if(samples == null) {
            n = y.length;
            samples = new int[n];
            for(int i = 0; i < n; i++) {
                samples[i] = 1;
                sum += y[i];
            }
        } else {
            for(int i = 0; i < y.length; i++) {
                n += samples[i];
                sum += samples[i] * y[i];
            }
        }

        this.root = new Node(sum / n);

        TrainNode trainRoot = new TrainNode(root, x, y, samples);
        if(J == Integer.MAX_VALUE) {
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
            for(int leaves = 1; leaves < this.J; leaves++) {
                // parent is the leaf to split
                TrainNode node = nextSplits.poll();
                if(node == null) {
                    break;
                }
                node.split(nextSplits); // Split the parent node into two children nodes
            }
        }

        if(output != null) {
            trainRoot.calculateOutput(output);
        }
    }

    /**
     * Returns the variable importance. Every time a split of a node is made on
     * variable the impurity criterion for the two descendent nodes is less than
     * the parent node. Adding up the decreases for each individual variable
     * over the tree gives a simple measure of variable importance.
     *
     * @return the variable importance
     */
    public double[] importance() {
        return importance;
    }

    @Override
    public double predict(double[] x) {
        return root.predict(x);
    }

    public String predictCodegen() {
        StringBuilder buf = new StringBuilder(1024);
        root.codegen(buf, 0);
        return buf.toString();
    }

    public String predictOpCodegen(@Nonnull String sep) {
        List<String> opslist = new ArrayList<String>();
        root.opcodegen(opslist, 0);
        opslist.add("call end");
        String scripts = StringUtils.concat(opslist, sep);
        return scripts;
    }

}
