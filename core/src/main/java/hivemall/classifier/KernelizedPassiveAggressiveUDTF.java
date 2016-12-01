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
    private FloatArrayList alpha;
    private List<FeatureValue[]> supportVectors;
    private float loss;
    //PA1, PA2
    private boolean pa1, pa2;
    private float c;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if (numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException(
                "KernelizedPassiveAggressiveUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, int label [, constant string options]");
        }
        processOptions(argOIs);
        supportVectors = new ArrayList<FeatureValue[]>();
        alpha = new FloatArrayList();
        return super.initialize(argOIs);
    }

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("pkc", "polynomialkernelconstant", true,
            "Constant c inside polynomial kernel K = (dot(xi,xj) + c)^d [default 1.0]");
        opts.addOption("d", "polynomialkerneldegree", true,
            "Degree of polynomial kernel d [default 2]");
        opts.addOption("pa1", "PA-1", false, "Whether to use PA-1 for calculating loss");
        opts.addOption("pa2", "PA-2", false, "Whether to use PA-2 for calculating loss");
        opts.addOption("c", "aggressiveness", true,
            "Aggressiveness parameter C for PA-1 and PA-2 [default 1.0]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);
        float pkc = 1.f;
        int degree = 2;
        float c = 1.f;
        pa1 = false;
        pa2 = false;
        if (cl != null) {
            pa1 = cl.hasOption("pa1");
            pa2 = cl.hasOption("pa2");
            if (pa1 && pa2) {
                throw new UDFArgumentException("PA-1 and PA-2 cannot be simultaneously set.");
            }
            String pkc_str = cl.getOptionValue("pkc");
            String d_str = cl.getOptionValue("d");
            String c_str = cl.getOptionValue("c");
            if (pkc_str != null) {
                pkc = Float.parseFloat(pkc_str);
            }
            if (d_str != null) {
                degree = Integer.parseInt(d_str);
                if (!(degree >= 1)) {
                    throw new UDFArgumentException(
                        "Polynomial Kernel Degree d must be d >= 1: " + degree);
                }
            }
            if (c_str != null) {
                c = Float.parseFloat(c_str);
                if (!(c > 0.f)) {
                    throw new UDFArgumentException(
                        "Aggressiveness parameter C must be C > 0: " + c);
                }
            }
        }
        this.pkc = pkc;
        this.degree = degree;
        this.c = c;

        return cl;
    }

    float getLoss() {//only used for testing purposes at the moment
        return loss;
    }

    @Override
    protected void train(FeatureValue[] features, int label) {
        train(supportVectors, alpha, features, label);
        supportVectors.add(features);
    }

    protected void train(@Nullable List<FeatureValue[]> supportVectors, //Null when kernel expansion enabled
            @Nonnull FloatArrayList alpha, @Nonnull final FeatureValue[] features,
            final int label) {
        final float y = label > 0 ? 1.f : -1.f;

        PredictionResult margin = calcScoreWithKernelAndNorm(supportVectors, alpha, features);
        float p = margin.getScore();
        float loss = LossFunctions.hingeLoss(p, y); // 1.0 - y * p
        this.loss = loss;

        updateKernel(y, loss, margin, features);
    }

    @Nonnull
    protected PredictionResult calcScoreWithKernelAndNorm(
            @Nonnull final List<FeatureValue[]> supportVectors, @Nonnull FloatArrayList alpha,
            @Nonnull final FeatureValue[] features) {
        float score = 0.f;
        float squared_norm = 0.f;

        for (int i = 0; i < supportVectors.size(); ++i) {
            FeatureValue[] sv = supportVectors.get(i);
            float currentAlpha = alpha.get(i);
            score += currentAlpha * polynomialKernel(features, sv, pkc, degree);
        }
        for (FeatureValue fv : features) {
            if (fv == null) {
                continue;
            }
            float v = fv.getValueAsFloat();
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
            float eta = pa1 ? eta1(loss, margin) : pa2 ? eta2(loss, margin) : eta(loss, margin);
            float diff = eta * label;
            alpha.add(diff);
        } else {
            alpha.add(0.f);
        }
    }

    /** returns learning rate */
    protected float eta(float loss, PredictionResult margin) {
        return loss / margin.getSquaredNorm();
    }

    protected float eta1(float loss, PredictionResult margin) {
        float squared_norm = margin.getSquaredNorm();
        float eta = loss / squared_norm;
        return Math.min(c, eta);
    }

    protected float eta2(float loss, PredictionResult margin) {
        float squared_norm = margin.getSquaredNorm();
        float eta = loss / (squared_norm + (0.5f / c));
        return eta;
    }

    float getAlpha(int index) {
        return alpha.get(index);
    }

    public static class PolynomialKernelInvertedKPA extends KernelizedPassiveAggressiveUDTF {
        private Map<Object, BitSet> supportVectorsIndicesPKI;

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            supportVectorsIndicesPKI = new HashMap<Object, BitSet>();
            return super.initialize(argOIs);
        }

        @Override
        protected void train(FeatureValue[] features, int label) {
            BitSet svSharedIndices = new BitSet();
            for (FeatureValue fv : features) {
                Object feature = fv.getFeature();
                BitSet matches = supportVectorsIndicesPKI.get(feature);
                if (matches != null) {
                    svSharedIndices.or(matches);
                }
            }
            int size = svSharedIndices.cardinality();
            List<FeatureValue[]> supportVectorsPKI = new ArrayList<FeatureValue[]>(size);
            FloatArrayList alpha = new FloatArrayList(size);
            for (int i = svSharedIndices.nextSetBit(0); i >= 0; i =
                    svSharedIndices.nextSetBit(i + 1)) {
                FeatureValue[] sv = super.supportVectors.get(i);
                supportVectorsPKI.add(sv);
                alpha.add(super.alpha.get(i));
            }

            train(supportVectorsPKI, alpha, features, label);

            int nextIndex = super.supportVectors.size();
            for (FeatureValue fv : features) {
                Object feature = fv.getFeature();
                BitSet bitset = supportVectorsIndicesPKI.get(feature);
                if (bitset == null) {
                    bitset = new BitSet();
                    supportVectorsIndicesPKI.put(feature, bitset);
                }
                bitset.set(nextIndex);
            }
            super.supportVectors.add(features);
        }
    }

    public static class KernelExpansionKPA extends KernelizedPassiveAggressiveUDTF {
        private float w0;//sum over features (kernel constant * kernel weight(alpha))
        private HashMap<Object, Double> w1;//sum over features (value of feature * kernel weight)
        private HashMap<Object, Double> w2;//sum over features (value of feature^2 * kernel weight)
        private HashMap<Object, Double> w3;//sum over feature pairs (value of feature1 * value of feature2 * kernel weight)

        @Override
        public StructObjectInspector initialize(ObjectInspector[] argOIs)
                throws UDFArgumentException {
            w0 = 0.f;
            w1 = new HashMap<Object, Double>();
            w2 = new HashMap<Object, Double>();
            w3 = new HashMap<Object, Double>();
            return super.initialize(argOIs);
        }

        @Override
        protected void train(FeatureValue[] features, int label) {
            train(null, super.alpha, features, label);
        }

        @Override
        protected PredictionResult calcScoreWithKernelAndNorm(List<FeatureValue[]> supportVectors,
                FloatArrayList alpha, FeatureValue[] features) {
            float score = 0.f;
            float norm = 0.f;
            for (int i = 0; i < features.length; ++i) {
                float tempScore = 0.f;
                if (features[i] == null) {
                    continue;
                }
                String key = features[i].getFeature().toString();
                Double score1 = w1.get(key);
                Double score2 = w2.get(key);
                double val = features[i].getValue();
                double v2 = val * val;
                if (score1 == null) {
                    w1.put(key, 0.d);
                } else {
                    tempScore += val * score1.doubleValue();
                }
                if (score2 == null) {
                    w2.put(key, 0.d);
                } else {
                    tempScore += v2 * score2.doubleValue();
                }
                norm += v2;
                for (int j = i + 1; j < features.length; ++j) {
                    String key2 = key + ":" + features[j].getFeature().toString();
                    Double score3 = w3.get(key2);
                    double valJ = features[j].getValue();
                    if (score3 == null) {
                        w3.put(key2, 0.d);
                    } else {
                        tempScore += val * valJ * score3.doubleValue();
                    }
                }
                score += tempScore;
            }
            score += w0;
            return new PredictionResult(score).squaredNorm(norm);
        }

        @Override
        protected void updateKernel(float label, float loss, PredictionResult margin,
                @Nonnull FeatureValue[] features) {
            if (loss > 0.f) { // y * p < 1
                float eta = super.pa1 ? eta1(loss, margin)
                        : super.pa2 ? eta2(loss, margin) : eta(loss, margin);
                float diff = eta * label;
                expandKernel(features, diff);
                super.alpha.add(diff);//unnecessary (only for testing purposes)
            }
        }

        private void expandKernel(FeatureValue[] supportVector, float alpha) {
            int deg = super.degree;
            if (deg != 2) {
                throw new UnsupportedOperationException(
                    "Quadratic kernel expansion must only be used with d = 2. d: " + deg);
            }
            w0 += alpha * super.pkc * super.pkc;//addToW0
            addToW12(supportVector, alpha);
            addToW3(supportVector, alpha);
        }

        private void addToW12(FeatureValue[] supportVector, float alpha) {
            for (FeatureValue s : supportVector) {
                if (s == null) {
                    continue;
                }
                String key = s.getFeature().toString();
                double currentValue = s.getValue();
                double increment = alpha * currentValue;
                w1.put(key, w1.get(key) + 2.d * super.pkc * increment);
                increment *= currentValue;
                w2.put(key, w2.get(key) + increment);
            }
        }

        private void addToW3(FeatureValue[] supportVector, float alpha) {
            for (int j = 0; j < supportVector.length; ++j) {
                for (int k = j + 1; k < supportVector.length; ++k) {
                    FeatureValue sj = supportVector[j];
                    FeatureValue sk = supportVector[k];
                    if (sj == null || sk == null) {
                        continue;
                    }
                    String key = sj.getFeature().toString() + ":" + sk.getFeature().toString();
                    double valueJK = w3.get(key);
                    double svJ = sj.getValue();
                    double svK = sk.getValue();
                    w3.put(key, valueJK + 2.d * alpha * svJ * svK);
                }
            }
        }
    }
}
