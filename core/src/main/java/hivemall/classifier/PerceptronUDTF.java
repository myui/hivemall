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

import hivemall.model.FeatureValue;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "train_perceptron",
        value = "_FUNC_(list<string|int|bigint> features, int label [, const string options])"
                + " - Returns a relation consists of <string|int|bigint feature, float weight>",
        extended = "Build a prediction model by Perceptron binary classifier")
public final class PerceptronUDTF extends BinaryOnlineClassifierUDTF {

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
    protected void update(@Nonnull final FeatureValue[] features, float y, float p) {
        update(features, y);
    }
}
