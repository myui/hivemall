/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.classifier;

import hivemall.common.LossFunctions;
import hivemall.model.FeatureValue;
import hivemall.model.PredictionResult;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "train_pa",
        value = "_FUNC_(list<string|int|bigint> features, int label [, const string options])"
                + " - Returns a relation consists of <string|int|bigint feature, float weight>",
        extended = "Build a prediction model by Passive-Aggressive (PA) binary classifier")
public class PassiveAggressiveUDTF extends BinaryOnlineClassifierUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if (numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException(
                "_FUNC_ takes 2 or 3 arguments: List<Text|Int|BitInt> features, int label [, constant string options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected void train(@Nonnull final FeatureValue[] features, final int label) {
        final float y = label > 0 ? 1.f : -1.f;

        PredictionResult margin = calcScoreAndNorm(features);
        float p = margin.getScore();
        float loss = LossFunctions.hingeLoss(p, y); // 1.0 - y * p

        if (loss > 0.f) { // y * p < 1
            float eta = eta(loss, margin);
            float coeff = eta * y;
            update(features, coeff);
        }
    }

    /** returns learning rate */
    protected float eta(float loss, PredictionResult margin) {
        return loss / margin.getSquaredNorm();
    }

    @Description(
            name = "train_pa1",
            value = "_FUNC_(list<string|int|bigint> features, int label [, const string options])"
                    + " - Returns a relation consists of <string|int|bigint feature, float weight>",
            extended = "Build a prediction model by Passive-Aggressive 1 (PA-1) binary classifier")
    public static class PA1 extends PassiveAggressiveUDTF {

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
                        throw new UDFArgumentException("Aggressiveness parameter C must be C > 0: "
                                + c);
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

    @Description(
            name = "train_pa2",
            value = "_FUNC_(list<string|int|bigint> features, int label [, const string options])"
                    + " - Returns a relation consists of <string|int|bigint feature, float weight>",
            extended = "Build a prediction model by Passive-Aggressive 2 (PA-2) binary classifier")
    public static class PA2 extends PA1 {

        @Override
        protected float eta(float loss, PredictionResult margin) {
            float squared_norm = margin.getSquaredNorm();
            float eta = loss / (squared_norm + (0.5f / c));
            return eta;
        }
    }
}
