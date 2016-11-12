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
package hivemall.mix;

public abstract class MixedWeight {

    private float weight;

    public MixedWeight(float weight) {
        this.weight = weight;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public int getDeltaUpdates() {
        return 0; // dummy
    }

    public float getCovar() {
        return 0.f; // dummy
    }

    public void setDeltaUpdates(int deltaUpdates) {
        throw new UnsupportedOperationException();
    }

    public void setCovar(float covar) {
        throw new UnsupportedOperationException();
    }

    public static final class WeightWithDelta extends MixedWeight {

        private byte deltaUpdates;

        public WeightWithDelta(float weight, int deltaUpdates) {
            super(weight);
            this.deltaUpdates = (byte) deltaUpdates;
        }

        @Override
        public int getDeltaUpdates() {
            return deltaUpdates;
        }

        @Override
        public void setDeltaUpdates(int deltaUpdates) {
            this.deltaUpdates = (byte) deltaUpdates;
        }

    }

    public static final class WeightWithCovar extends MixedWeight {

        private float covar;

        public WeightWithCovar(float weight, float covar) {
            super(weight);
            this.covar = covar;
        }

        @Override
        public float getCovar() {
            return covar;
        }

        @Override
        public void setCovar(float covar) {
            this.covar = covar;
        }

    }

}
