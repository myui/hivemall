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
package hivemall.smile.utils;

import hivemall.smile.classification.DecisionTree.SplitRule;
import hivemall.smile.data.NominalAttribute2;

import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import smile.data.Attribute;
import smile.data.DateAttribute;
import smile.data.NominalAttribute;
import smile.data.NumericAttribute;
import smile.data.StringAttribute;
import smile.math.Math;

public final class SmileExtUtils extends smile.util.SmileUtils {

    private SmileExtUtils() {}

    /**
     * Q for {@link NumericAttribute}, C for {@link NominalAttribute}, S for
     * {@link StringAttribute}, and D for {@link DateAttribute}.
     */
    @Nullable
    public static Attribute[] resolveAttributes(@Nullable final String opt)
            throws UDFArgumentException {
        if(opt == null) {
            return null;
        }
        final String[] opts = opt.split(",");
        final int size = opts.length;
        final Attribute[] attr = new Attribute[size];
        for(int i = 0; i < size; i++) {
            final String type = opts[i];
            if("Q".equals(type)) {
                attr[i] = new NumericAttribute("V" + (i + 1));
            } else if("C".equals(type)) {
                attr[i] = new NominalAttribute2("V" + (i + 1));
            } else if("S".equals(type)) {
                attr[i] = new StringAttribute("V" + (i + 1));
            } else if("D".equals(type)) {
                attr[i] = new DateAttribute("V" + (i + 1));
            } else {
                throw new UDFArgumentException("Unexpected type: " + type);
            }
        }
        return attr;
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

    @Nonnull
    public static Attribute[] attributeTypes(@Nullable Attribute[] attributes, @Nonnull final double[][] x) {
        if(attributes == null) {
            int p = x[0].length;
            attributes = new Attribute[p];
            for(int i = 0; i < p; i++) {
                attributes[i] = new NumericAttribute("V" + (i + 1));
            }
        } else {
            int size = attributes.length;
            for(int j = 0; j < size; j++) {
                Attribute attr = attributes[j];
                if(attr instanceof NominalAttribute2) {
                    int max_x = 0;
                    for(int i = 0; i < x.length; i++) {
                        int x_ij = (int) x[i][j];
                        if(x_ij > max_x) {
                            max_x = x_ij;
                        }
                    }
                    ((NominalAttribute2) attr).setSize(max_x + 1);
                }
            }
        }
        return attributes;
    }

    @Nonnull
    public static SplitRule resolveSplitRule(@Nullable String ruleName) {
        if("gini".equalsIgnoreCase(ruleName)) {
            return SplitRule.GINI;
        } else if("entropy".equalsIgnoreCase(ruleName)) {
            return SplitRule.ENTROPY;
        } else {
            return SplitRule.GINI;
        }
    }

    public static int computeNumInputVars(final float numVars, final double[][] x) {
        final int numInputVars;
        if(numVars <= 0.f) {
            int dims = x[0].length;
            numInputVars = (int) Math.round(Math.max(Math.sqrt(dims), dims / 3.0d));
        } else if(numVars > 0.f && numVars <= 1.f) {
            numInputVars = (int) (numVars * x[0].length);
        } else {
            numInputVars = (int) numVars;
        }
        return numInputVars;
    }

    public static long generateSeed() {
        return Thread.currentThread().getId() * System.currentTimeMillis();
    }

    public static void shuffle(@Nonnull final int[] x, @Nonnull final smile.math.Random rnd) {
        for(int i = x.length; i > 1; i--) {
            int j = rnd.nextInt(i);
            Math.swap(x, i - 1, j);
        }
    }

    public static void shuffle(@Nonnull final double[][] x, final int[] y, @Nonnull long seed) {
        if(x.length != y.length) {
            throw new IllegalArgumentException("x.length (" + x.length + ") != y.length ("
                    + y.length + ')');
        }
        if(seed == -1L) {
            seed = generateSeed();
        }
        final smile.math.Random rnd = new smile.math.Random(seed);
        for(int i = x.length; i > 1; i--) {
            int j = rnd.nextInt(i);
            swap(x, i - 1, j);
            Math.swap(y, i - 1, j);
        }
    }

    public static void shuffle(@Nonnull final double[][] x, final double[] y, @Nonnull long seed) {
        if(x.length != y.length) {
            throw new IllegalArgumentException("x.length (" + x.length + ") != y.length ("
                    + y.length + ')');
        }
        if(seed == -1L) {
            seed = generateSeed();
        }
        final smile.math.Random rnd = new smile.math.Random(seed);
        for(int i = x.length; i > 1; i--) {
            int j = rnd.nextInt(i);
            swap(x, i - 1, j);
            Math.swap(y, i - 1, j);
        }
    }

    /**
     * Swap two elements of an array.
     */
    public static void swap(final double[][] x, final int i, final int j) {
        double[] s = x[i];
        x[i] = x[j];
        x[j] = s;
    }

}
