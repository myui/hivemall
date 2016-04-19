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

import hivemall.model.Solver;
import hivemall.model.Solver.SolverType;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Map;

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

        private float[] sum_of_squared_gradients;

        // Reused to use `AdaGradSolver.updateWeightValue`
        private WeightValue.WeightValueParamsF1 weightValueReused;

        public AdaGradSolver(int ndims, Map<String, String> solverOptions) {
            super(solverOptions);
            sum_of_squared_gradients = new float[ndims];
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setSumOfSquaredGradients(sum_of_squared_gradients[i]);
            computeUpdateValue(weightValueReused, xi, gradient);
            sum_of_squared_gradients[i] = weightValueReused.getSumOfSquaredGradients();
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

        private float[] val_m;
        private float[] val_v;

        // Reused to use `Adam.updateWeightValue`
        private WeightValue.WeightValueParamsF2 weightValueReused;

        public AdamSolver(int ndims, Map<String, String> solverOptions) {
            super(solverOptions);
            val_m = new float[ndims];
            val_v = new float[ndims];
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setM(val_m[i]);
            weightValueReused.setV(val_v[i]);
            computeUpdateValue(weightValueReused, xi, gradient);
            val_m[i] = weightValueReused.getM();
            val_v[i] = weightValueReused.getV();
            return weightValueReused.get();
        }

        private void ensureCapacity(final int index) {
            if(index >= val_m.length) {
                int bits = MathUtils.bitsRequired(index);
                int newSize = (1 << bits) + 1;
                this.val_m = Arrays.copyOf(val_m, newSize);
                this.val_v = Arrays.copyOf(val_v, newSize);
            }
        }

    }

}
