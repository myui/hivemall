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
package hivemall.classifier;

import hivemall.common.LossFunctions;
import hivemall.model.FeatureValue;
import hivemall.model.PredictionResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class KernelizedPassiveAggressiveUDTF extends BinaryOnlineClassifierUDTF {

    private float a;
    private int degree;
    private int capSV;
    private float loss;
    private boolean pki;
    private HashMap<String, ArrayList<Integer>> supportVectorsIndicesPKI;
    private ArrayList<FeatureValue[]> supportVectors;

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("a", "kernelconstant", true,
            "Constant a inside polynomial kernel K = (dot(xi,xj) + a)^d [default 1.0]");
        opts.addOption("d", "degree", true, "Degree of polynomial kernel d [default 2]");
        opts.addOption("PKI", "invertedindex", false,
            "Whether to use inverted index maps for finding support vectors (better time complexity, worse spacial complexity) [default: OFF]");
        opts.addOption("capSV", "supportvectorcapacity", true, "Maximum number of support vectors to keep [default 1000]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);
        float a = 1.f;
        int degree = 2;
        int capSV = 1000;
        this.pki = false;
        if (cl != null) {
            String a_str = cl.getOptionValue("a");
            String d_str = cl.getOptionValue("d");
            String m_str = cl.getOptionValue("capSV");
            this.pki = cl.hasOption("PKI");
            if (a_str != null) {
                a = Float.parseFloat(a_str);
            }
            if (d_str != null) {
                degree = Integer.parseInt(d_str);
                if (!(degree >= 1)) {
                    throw new UDFArgumentException(
                        "Polynomial Kernel Degree d must be d >= 1: " + degree);
                }
            }
            if (m_str != null) {
                capSV = Integer.parseInt(m_str);
                if (capSV <= 0) {
                    capSV = Integer.MAX_VALUE;
                }
            }
        }
        if (this.pki) {
            supportVectorsIndicesPKI = new HashMap<String, ArrayList<Integer>>();
        }

        this.a = a;
        this.degree = degree;
        this.capSV = capSV;
        return cl;
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if (numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException(
                "KernelizedPassiveAggressiveUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, int label [, constant string options]");
        }
        processOptions(argOIs);
        supportVectors = new ArrayList<FeatureValue[]>();
        return super.initialize(argOIs);
    }
    
    float getLoss() {//only used for testing purposes at the moment
        return loss;
    }

    private ArrayList<Integer> mergeSortedSets(ArrayList<Integer> left,
            ArrayList<Integer> right) {
        int leftSize = left.size();
        if (leftSize == 0) {
            return right;
        }
        int rightSize = right.size();
        if (rightSize == 0) {
            return left;
        }
        int i = 0;
        int j = 0;
        ArrayList<Integer> merged;
        try {
            merged = new ArrayList<Integer>(leftSize + rightSize);
        } catch (IllegalArgumentException e) {
            merged = new ArrayList<Integer>();
        }
        while(i < leftSize && j < rightSize) {
            int b = left.get(i);
            int s = right.get(j);
            if (b < s) {
                merged.add(b);
                ++i;
            } else if (b > s) {
                merged.add(s);
                ++j;
            } else if (b == s) {
                merged.add(b);
                ++i;
                ++j;
            }
        }
        if (i < leftSize) {
            for (; i < leftSize; ++i) {
                merged.add(left.get(i));
            }
        }
        if (j < rightSize) {
            for (; j < rightSize; ++j) {
                merged.add(right.get(j));
            }
        }
        return merged;
    }

    @Override
    protected void train(FeatureValue[] features, int label) {
        if (pki) {
            ArrayList<Integer> matchingSupportVectors = new ArrayList<Integer>();
            for (FeatureValue f : features) {
                ArrayList<Integer> matches = supportVectorsIndicesPKI.get(f.getFeature().toString());
                if (matches != null) {
                    matchingSupportVectors = mergeSortedSets(matchingSupportVectors, matches);
                } //sets must be combined to prevent doubling up
            }
            ArrayList<FeatureValue[]> supportVectorsPKI = new ArrayList<FeatureValue[]>();
            for (int i : matchingSupportVectors) {
                supportVectorsPKI.add(supportVectors.get(i));
            }
            train(supportVectorsPKI, features, label);
        } else {
            train(supportVectors, features, label);
        }
    }

    protected void train(@Nonnull ArrayList<FeatureValue[]> supportVectors,
            @Nonnull final FeatureValue[] features, final int label) {
        final float y = label > 0 ? 1.f : -1.f;

        PredictionResult margin = calcScoreWithKernelAndNorm(supportVectors, features, a, degree);
        float p = margin.getScore();
        float loss = LossFunctions.hingeLoss(p, y); // 1.0 - y * p
        this.loss = loss;

        if (loss > 0.f) { // y * p < 1
            float eta = eta(loss, margin);
            float diff = eta * y;
            updateKernelWeights(features, diff);
        }

        if(this.supportVectors.size() <= capSV) {
            if (pki) {
                int location = this.supportVectors.size();
                for (FeatureValue f : features) {
                    String featStr = f.getFeature().toString();
                    ArrayList<Integer> indexList = supportVectorsIndicesPKI.get(featStr);
                    if (indexList == null) {
                        ArrayList<Integer> newIndexList = new ArrayList<Integer>();
                        newIndexList.add(location);
                        supportVectorsIndicesPKI.put(featStr, newIndexList);
                    } else {
                        indexList.add(location);
                    }
                }
            }
            this.supportVectors.add(features);
        }
    }

    /** returns learning rate */
    protected float eta(float loss, PredictionResult margin) {
        return loss / margin.getSquaredNorm();
    }

    public static class KPA1 extends KernelizedPassiveAggressiveUDTF {
        /** Aggressiveness parameter */
        protected float c;

        @Override
        protected Options getOptions() {
            Options opts = super.getOptions();
            opts.addOption("c", "aggressiveness", true, "Aggressiveness parameter C [default 1.0]");
            return opts;
        }

        @Override
        protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
            final CommandLine cl = super.processOptions(argOIs);

            float c = 1.f;
            if (cl != null) {
                String c_str = cl.getOptionValue("c");
                if (c_str != null) {
                    c = Float.parseFloat(c_str);
                    if (!(c > 0.f)) {
                        throw new UDFArgumentException(
                            "Aggressiveness parameter C must be C > 0: " + c);
                    }
                }
            }

            this.c = c;
            return cl;
        }

        @Override
        protected float eta(float loss, PredictionResult margin) {
            float squared_norm = margin.getSquaredNorm();
            float eta = loss / squared_norm;
            return Math.min(c, eta);
        }

    }

    public static class KPA2 extends KPA1 {
        @Override
        protected float eta(float loss, PredictionResult margin) {
            float squared_norm = margin.getSquaredNorm();
            float eta = loss / (squared_norm + (0.5f / c));
            return eta;
        }
    }
}
