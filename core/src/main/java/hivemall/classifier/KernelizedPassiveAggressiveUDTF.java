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
import hivemall.utils.collections.FloatArrayList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class KernelizedPassiveAggressiveUDTF extends BinaryOnlineClassifierUDTF {

    private float pkc;
    private int degree;
    private float loss;
    private boolean pki;
    private Map<Object, BitSet> supportVectorsIndicesPKI;
    private List<FeatureValue[]> supportVectors;
    private FloatArrayList alpha;

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("pkc", "polynomialkernelconstant", true,
            "Constant c inside polynomial kernel K = (dot(xi,xj) + c)^d [default 1.0]");
        opts.addOption("d", "polynomialkerneldegree", true,
            "Degree of polynomial kernel d [default 2]");
        opts.addOption("PKI", "kernelindexinverted", false,
            "Whether to use inverted index maps for finding support vectors (conflicts with kernel expansion) [default: OFF]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);
        float pkc = 1.f;
        int degree = 2;
        if (cl != null) {
            String a_str = cl.getOptionValue("a");
            String d_str = cl.getOptionValue("d");
            if (a_str != null) {
                pkc = Float.parseFloat(a_str);
            }
            if (d_str != null) {
                degree = Integer.parseInt(d_str);
                if (!(degree >= 1)) {
                    throw new UDFArgumentException(
                        "Polynomial Kernel Degree d must be d >= 1: " + degree);
                }
            }
        }
        if (pki) {
            supportVectorsIndicesPKI = new HashMap<Object, BitSet>();
        }
        alpha = new FloatArrayList();

        this.pkc = pkc;
        this.degree = degree;
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

    @Override
    protected void train(FeatureValue[] features, int label) {
        if (this.pki) {
            BitSet svIndex = new BitSet();
            for (FeatureValue fv : features) {
                Object feature = fv.getFeature();
                BitSet matches = supportVectorsIndicesPKI.get(feature);
                if (matches != null) {
                    svIndex.or(matches);
                }
            }
            int size = svIndex.cardinality();
            List<FeatureValue[]> supportVectorsPKI = new ArrayList<FeatureValue[]>(size);
            FloatArrayList alpha = new FloatArrayList(size);
            for (int i = svIndex.nextSetBit(0); i >= 0; i = svIndex.nextSetBit(i + 1)) {
                FeatureValue[] sv = supportVectors.get(i);
                supportVectorsPKI.add(sv);
                alpha.add(this.alpha.get(i));
            }
            train(supportVectorsPKI, alpha, features, label);
        } else {
            train(supportVectors, this.alpha, features, label);
        }

        if (pki) {
            int svIndex = supportVectors.size();
            for (FeatureValue fv : features) {
                Object feature = fv.getFeature();
                BitSet bitset = supportVectorsIndicesPKI.get(feature);
                if (bitset == null) {
                    bitset = new BitSet();
                    supportVectorsIndicesPKI.put(feature, bitset);
                }
                bitset.set(svIndex);
            }
        }
        this.supportVectors.add(features);
    }

    protected void train(@Nullable List<FeatureValue[]> supportVectors, //Null when kernel expansion enabled
            @Nonnull FloatArrayList alpha, @Nonnull final FeatureValue[] features,
            final int label) {
        final float y = label > 0 ? 1.f : -1.f;

        PredictionResult margin =
                calcScoreWithKernelAndNorm(supportVectors, alpha, features, pkc, degree);
        float p = margin.getScore();
        float loss = LossFunctions.hingeLoss(p, y); // 1.0 - y * p
        this.loss = loss;

        updateKernel(y, loss, margin, features);
    }

    @Nonnull
    protected PredictionResult calcScoreWithKernelAndNorm(
            @Nonnull final List<FeatureValue[]> supportVectors, @Nonnull FloatArrayList alpha,
            @Nonnull final FeatureValue[] features, float a, int degree) {
        float score = 0.f;
        float squared_norm = 0.f;

        for (int i = 0; i < supportVectors.size(); ++i) {
            FeatureValue[] sv = supportVectors.get(i);
            float currentAlpha = alpha.get(i);
            score += currentAlpha * polynomialKernel(features, sv, a, degree);
        }
        for (FeatureValue f : features) {
            if (f == null) {
                continue;
            }
            float v = f.getValueAsFloat();
            squared_norm += (v * v);
        }

        return new PredictionResult(score).squaredNorm(squared_norm);
    }

    private static double polynomialKernel(@Nonnull final FeatureValue[] fv1,
            @Nonnull final FeatureValue[] fv2, final float c, final int degree) {
        double ret = 0.d;
        int i = 0;
        int j = 0;
        while (i < fv1.length && j < fv2.length) {
            FeatureValue a = fv1[i];
            FeatureValue b = fv2[j];
            if (a == null) {
                ++i;
                continue;
            }
            if (b == null) {
                ++j;
                continue;
            }
            final int cmp = a.compareTo(b);
            if (cmp < 0) {
                ++i;
            } else if (cmp > 0) {
                ++j;
            } else if (cmp == 0) {
                ret += a.getValue() * b.getValue();
                ++i;
                ++j;
            }
        }
        if (ret == 0.d) {
            return 0.d;
        }
        ret = Math.pow(ret + c, degree);
        return ret;
    }

    protected void updateKernel(final float label, final float loss, final PredictionResult margin,
            FeatureValue[] features) {
        if (loss > 0.f) { // y * p < 1
            float eta = eta(loss, margin);
            float diff = eta * label;
            this.alpha.add(diff);
        } else {
            this.alpha.add(0.f);
        }
    }


    /** returns learning rate */
    protected float eta(float loss, PredictionResult margin) {
        return loss / margin.getSquaredNorm();
    }

    float getAlpha(int index) {
        return this.alpha.get(index);
    }

    protected float getPKC() {
        return pkc;
    }

    protected int getDegree() {
        return degree;
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

    public class KernelExpansionKPA extends KernelizedPassiveAggressiveUDTF {
        private boolean exp2;
        private float w0;
        private HashMap<Object, Double> w1;
        private HashMap<Object, Double> w2;
        private HashMap<Object, Double> w3;

        @Override
        protected Options getOptions() {
            Options opts = super.getOptions();
            opts.addOption("exp", "kernelexpansion", false,
                "Whether to use quadratic kernel expansion for pre-calculating support vector kernels (conflicts with PKI; requires d = 2 (default value)) [default: OFF]");
            return opts;
        }

        @Override
        protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
            final CommandLine cl = super.processOptions(argOIs);
            if (cl != null) {
                this.exp2 = cl.hasOption("exp2");
            }
            if (super.pki && exp2) {
                throw new UDFArgumentException(
                    "PKI conflicts with kernel expansion; use option \"split\" to utilize both.");
            }
            alpha = new FloatArrayList();

            return cl;
        }

        @Override
        protected void train(FeatureValue[] features, int label) {
            train(null, alpha, features, label);
        }

        @Override
        protected PredictionResult calcScoreWithKernelAndNorm(List<FeatureValue[]> supportVectors,
                FloatArrayList alpha, FeatureValue[] features, float a, int degree) {//supportVectors unnecessary?
            float score = this.w0;
            float norm = 0.f;
            for (int i = 0; i < features.length; ++i) {
                String key = features[i].getFeature().toString();
                Double score1 = w1.get(key);
                Double score2 = w2.get(key);
                if (score1 == null) {
                    w1.put(key, 0.d);
                } else {
                    score += score1.doubleValue();
                }
                if (score2 == null) {
                    w2.put(key, 0.d);
                } else {
                    score += score2.doubleValue();
                }
                double val = features[i].getValue();
                norm += val * val;
                for (int j = i + 1; j < features.length; ++j) {
                    String key2 = key + features[j].getFeature().toString();
                    Double score3 = w3.get(key2);
                    if (score3 == null) {
                        w3.put(key2, 0.d);
                    } else {
                        score += score3.doubleValue();
                    }
                }
            }
            return new PredictionResult(score).squaredNorm(norm);
        }

        @Override
        protected void updateKernel(float label, float loss, PredictionResult margin,
                @Nonnull FeatureValue[] features) {
            if (loss > 0.f) { // y * p < 1
                float eta = eta(loss, margin);
                float diff = eta * label;
                expandKernel(features, diff);
            }
        }

        private void expandKernel(FeatureValue[] supportVector, float alpha) { 
            int deg = this.getDegree();
            if (deg != 2) {
                throw new UnsupportedOperationException(
                    "Quadratic kernel expansion must only be used with d = 2. d: " + deg);
            }
            this.w0 += alpha * this.getPKC();//addToW0
            addToW12(supportVector, alpha);
            addToW3(supportVector, alpha);
        }

        private void addToW12(FeatureValue[] supportVector, float alpha) {
            for (int j = 0; j < supportVector.length; ++j) {
                String key = supportVector[j].getFeature().toString();
                double currentValue = supportVector[j].getValue();
                double increment = alpha * currentValue;
                this.w1.put(key, this.w1.get(key) + increment);
                increment *= currentValue;
                this.w2.put(key, this.w2.get(key) + increment);
            }
        }

        private void addToW3(FeatureValue[] supportVector, float alpha) {
            for (int j = 0; j < supportVector.length; ++j) {
                for (int k = j + 1; k < supportVector.length; ++k) {
                    String key = supportVector[j].getFeature().toString()
                            + supportVector[k].getFeature().toString();
                    double valueJK = w3.get(key);
                    double svJ = supportVector[j].getValue();
                    double svK = supportVector[k].getValue();
                    this.w3.put(key, valueJK + alpha * svJ * svK);
                }
            }
        }
    }
}
