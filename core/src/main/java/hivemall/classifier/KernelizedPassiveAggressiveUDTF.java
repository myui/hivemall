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
    private boolean pki;
    private HashMap<String, HashSet<FeatureValue[]>> supportVectorsPKI;
    private ArrayList<FeatureValue[]> supportVectors;

    @Override
    protected Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("a", "kernelconstant", true,
            "Constant a inside polynomial kernel K = (dot(xi,xj) + a)^d [default 1.0]");
        opts.addOption("d", "degree", true, "Degree of polynomial kernel d [default 2]");
        opts.addOption("PKI", "invertedindex", false,
            "Whether to use inverted index maps for finding support vectors (better time complexity, worse spacial complexity) [default: OFF]");
        return opts;
    }

    @Override
    protected CommandLine processOptions(ObjectInspector[] argOIs) throws UDFArgumentException {
        final CommandLine cl = super.processOptions(argOIs);
        float a = 1.f;
        int degree = 2;
        boolean pki = false;
        if (cl != null) {
            String a_str = cl.getOptionValue("a");
            String d_str = cl.getOptionValue("d");
            pki = cl.hasOption("PKI");
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
        }
        if (pki) {
            supportVectorsPKI = new HashMap<String, HashSet<FeatureValue[]>>();
        } else {
            supportVectors = new ArrayList<FeatureValue[]>();
        }

        this.a = a;
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
        return super.initialize(argOIs);
    }

    @Override
    protected void train(FeatureValue[] features, int label) {
        if (pki) {
            HashSet<FeatureValue[]> matchingSupportVectors = new HashSet<FeatureValue[]>();
            for (FeatureValue f : features) {
                HashSet<FeatureValue[]> matches = supportVectorsPKI.get(f.getFeature().toString());
                if (matches != null) {
                    matchingSupportVectors.addAll(matches);
                } //sets must be combined to prevent doubling up
            }
            train(matchingSupportVectors, features, label);
        } else {
            train(this.supportVectors, features, label);
        }
    }

    protected void train(@Nonnull Collection<FeatureValue[]> supportVectors,
            @Nonnull final FeatureValue[] features, final int label) {
        final float y = label > 0 ? 1.f : -1.f;

        PredictionResult margin = calcScoreWithKernelAndNorm(supportVectors, features, a, degree);
        float p = margin.getScore();
        float loss = LossFunctions.hingeLoss(p, y); // 1.0 - y * p

        if (loss > 0.f) { // y * p < 1
            float eta = eta(loss, margin);
            float coeff = eta * y;
            update(features, coeff);
        }

        if (pki) {
            for (FeatureValue f : features) {
                supportVectorsPKI.get(f.getFeature().toString()).add(features);
            }
        } else {
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
