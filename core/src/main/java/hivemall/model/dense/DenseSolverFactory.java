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
package hivemall.model.dense;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Map;

import hivemall.model.Solver;
import hivemall.model.Solver.SolverType;
import hivemall.utils.unsafe.Platform;
import hivemall.model.WeightValue;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.math.MathUtils;

public final class DenseSolverFactory {

    public static Solver create(SolverType type, int ndims, @Nonnull Map<String, String> options) {
        switch (type) {
            case AdaDelta:
                // TODO: Need to implement
                throw new UnsupportedOperationException();

            case AdaGrad:
                return new AdaGradSolver(ndims, options);

            case Adam:
                return new AdamSolver(ndims, options);

            default:
                throw new IllegalArgumentException("Unsupported solver type");
        }
    }

    @NotThreadSafe
    static final class AdaGradSolver extends Solver.AdaGradSolver {

        private byte[] sum_of_squared_gradients;

        // Reused to use `AdaGradSolver.updateWeightValue`
        private WeightValue.WeightValueParamsF1 weightValueReused;

        public AdaGradSolver(int ndims, Map<String, String> solverOptions) {
            super(solverOptions);
            this.sum_of_squared_gradients = new byte[ndims * 4];
            this.weightValueReused = new WeightValue.WeightValueParamsF1(0.f, 0.f);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setSumOfSquaredGradients(Platform.getFloat(sum_of_squared_gradients, i * 4));
            computeUpdateValue(weightValueReused, xi, gradient);
            Platform.putFloat(sum_of_squared_gradients, i * 4, weightValueReused.getSumOfSquaredGradients());
            return weightValueReused.get();
        }

        private void ensureCapacity(final int index) {
            if(index >= sum_of_squared_gradients.length) {
                int bits = MathUtils.bitsRequired(index);
                int newSize = (1 << bits) + 1;
                this.sum_of_squared_gradients = Arrays.copyOf(sum_of_squared_gradients, newSize);
            }
        }

    }

    @NotThreadSafe
    static final class AdamSolver extends Solver.AdamSolver {

        // Store a sequence of pairs (val_m, val_v)
        private byte[] data;
        private int ndims;

        // Reused to use `Adam.updateWeightValue`
        private WeightValue.WeightValueParamsF2 weightValueReused;

        public AdamSolver(int ndims, Map<String, String> solverOptions) {
            super(solverOptions);
            this.data = new byte[ndims * 8];
            this.ndims = ndims;
            this.weightValueReused = new WeightValue.WeightValueParamsF2(0.f, 0.f, 0.f);
        }

        private float getM(int index) {
            return Platform.getFloat(data, index * 8);
        }
        private void setM(int index, float value) {
            Platform.putFloat(data, index * 8, value);
        }
        private float getV(int index) {
            return Platform.getFloat(data, index * 8 + 4);
        }
        private void setV(int index, float value) {
            Platform.putFloat(data, index * 8 + 4, value);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setM(getM(i));
            weightValueReused.setV(getV(i));
            computeUpdateValue(weightValueReused, xi, gradient);
            setM(i, weightValueReused.getM());
            setV(i, weightValueReused.getV());
            return weightValueReused.get();
        }

        private void ensureCapacity(final int index) {
            if(index >= this.ndims) {
                int bits = MathUtils.bitsRequired(index);
                int newSize = (1 << bits) + 1;
                this.data = Arrays.copyOf(data, newSize * 8);
            }
        }

    }

}
