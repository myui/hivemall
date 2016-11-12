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
package hivemall.classifier.multiclass;

import hivemall.model.FeatureValue;
import hivemall.model.Margin;

import javax.annotation.Nonnull;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(
        name = "train_multiclass_pa",
        value = "_FUNC_(list<string|int|bigint> features, {int|string} label [, const string options])"
                + " - Returns a relation consists of <{int|string} label, {string|int|bigint} feature, float weight>",
        extended = "Build a prediction model by Passive-Aggressive (PA) multiclass classifier")
public class MulticlassPassiveAggressiveUDTF extends MulticlassOnlineClassifierUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if (numArgs != 2 && numArgs != 3) {
            throw new UDFArgumentException(
                "MulticlassPassiveAggressiveUDTF takes 2 or 3 arguments: List<Text|Int|BitInt> features, {Int|Text} label [, constant text options]");
        }

        return super.initialize(argOIs);
    }

    @Override
    protected void train(@Nonnull final FeatureValue[] features, @Nonnull Object actual_label) {
        Margin margin = getMargin(features, actual_label);
        float loss = loss(margin);

        if (loss > 0.f) { // & missed_label != null
            float sqnorm = squaredNorm(features);
            if (sqnorm == 0.f) {// avoid divide by zero
                return;
            }
            float coeff = eta(loss, sqnorm);
            Object missed_label = margin.getMaxIncorrectLabel();
            update(features, coeff, actual_label, missed_label);
        }
    }

    protected float loss(Margin margin) {
        return 1.f - margin.get();
    }

    protected float eta(float loss, float sqnorm) {
        return loss / (2.f * sqnorm);
    }

    @Description(
            name = "train_multiclass_pa1",
            value = "_FUNC_(list<string|int|bigint> features, {int|string} label [, const string options])"
                    + " - Returns a relation consists of <{int|string} label, {string|int|bigint} feature, float weight>",
            extended = "Build a prediction model by Passive-Aggressive 1 (PA-1) multiclass classifier")
    public static class PA1 extends MulticlassPassiveAggressiveUDTF {

        /** Aggressiveness parameter */
        protected float c;

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
        protected float eta(float loss, float sqnorm) {
            float eta = loss / (2.f * sqnorm);
            return Math.min(c, eta);
        }

    }

    @Description(
            name = "train_multiclass_pa2",
            value = "_FUNC_(list<string|int|bigint> features, {int|string} label [, const string options])"
                    + " - Returns a relation consists of <{int|string} label, {string|int|bigint} feature, float weight>",
            extended = "Build a prediction model by Passive-Aggressive 2 (PA-2) multiclass classifier")
    public static final class PA2 extends PA1 {

        @Override
        protected float eta(float loss, float sqnorm) {
            float eta = loss / ((2.f * sqnorm) + (0.5f / c));
            return eta;
        }

    }
}
