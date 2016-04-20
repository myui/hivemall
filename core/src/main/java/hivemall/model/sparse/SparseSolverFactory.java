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
package hivemall.model.sparse;

import hivemall.model.Solver;
import hivemall.model.Solver.SolverType;
import hivemall.model.WeightValue;
import hivemall.utils.unsafe.Platform;
import hivemall.utils.unsafe.UnsafeOpenHashMap;

import javax.annotation.Nonnull;
import java.util.Map;

public final class SparseSolverFactory {

    public static Solver create(SolverType type, int size, @Nonnull Map<String, String> options) {
        switch (type) {
            case AdaDelta:
                // TODO: Need to implement
                throw new UnsupportedOperationException();

            case AdaGrad:
                return new AdaGradSolver(size, options);

            case Adam:
                return new AdamSolver(size, options);

            default:
                throw new IllegalArgumentException("Unsupported solver type");
        }
    }

    private static final class AdaGradSolver extends Solver.AdaGradSolver {

        private int size;

        // private final OpenHashMap<Object, Float> sum_of_squared_gradients;
        private Object[] keys;
        private byte[] sum_of_squared_gradients;

        // This hashmap maps `keys` to `sum_of_squared_gradients`
        private UnsafeOpenHashMap<Object> mapper;

        // Reused to use `AdaGradSolver.computeUpdateValue`
        private WeightValue.WeightValueParamsF1 weightValueReused;

        public AdaGradSolver(int size, Map<String, String> solverOptions) {
            super(solverOptions);
            this.size = size;
            this.mapper = new UnsafeOpenHashMap<Object>();
            int requiredSize = mapper.resize(size);
            this.keys = new Object[requiredSize];
            this.sum_of_squared_gradients = new byte[requiredSize * 4];
            mapper.reset(this.keys);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            int offset = mapper.get(feature);
            if (offset == -1) {
                offset = mapper.put(feature);
                if (offset == -1) {
                    // Make space bigger
                    reserveInternal(size * 2);
                    offset = mapper.put(feature);
                }
            }
            float sqg = Platform.getFloat(sum_of_squared_gradients, offset * 4);
            weightValueReused.set(weight);
            weightValueReused.setSumOfSquaredGradients(sqg);
            computeUpdateValue(weightValueReused, xi, gradient);
            Platform.putFloat(sum_of_squared_gradients, offset * 4, weightValueReused.getSumOfSquaredGradients());
            return weightValueReused.get();
        }

        private void reserveInternal(int size) {
            int requiredSize = mapper.resize(size);
            Object[] newKeys = new Object[requiredSize];
            byte[] newValues = new byte[requiredSize * 4];
            mapper.reset(newKeys);
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == null) continue;
                int newOffset = mapper.put(keys[i]);
                float oldValue = Platform.getFloat(keys, i * 4);
                Platform.putFloat(newValues, newOffset * 4, oldValue);
            }
            this.keys = newKeys;
            this.sum_of_squared_gradients = newValues;
            this.size = size;
        }
    }

    private static final class AdamSolver extends Solver.AdamSolver {

        private int size;

        // private final OpenHashMap<Object, Float> sum_of_squared_gradients;
        private Object[] keys;
        private byte[] values;

        // This hashmap maps `keys` to `sum_of_squared_gradients`
        private UnsafeOpenHashMap<Object> mapper;

        // Reused to use `AdamSolver.computeUpdateValue`
        private WeightValue.WeightValueParamsF1 weightValueReused;

        public AdamSolver(int size, Map<String, String> solverOptions) {
            super(solverOptions);
            this.size = size;
            this.mapper = new UnsafeOpenHashMap<Object>();
            int requiredSize = mapper.resize(size);
            this.keys = new Object[requiredSize];
            this.values = new byte[requiredSize * 8];
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            int offset = mapper.get(feature);
            if (offset == -1) {
                offset = mapper.put(feature);
                if (offset == -1) {
                    // Make space bigger
                    reserveInternal(size * 2);
                    offset = mapper.put(feature);
                }
            }
            float val_m = Platform.getFloat(values, offset * 8);
            float val_v = Platform.getFloat(values, offset * 8 + 4);
            weightValueReused.set(weight);
            weightValueReused.setV(val_m);
            weightValueReused.setV(val_v);
            computeUpdateValue(weightValueReused, xi, gradient);
            Platform.putFloat(values, offset * 8, val_m);
            Platform.putFloat(values, offset * 8 + 4, val_v);
            return weightValueReused.get();
        }

        private void reserveInternal(int size) {
            int requiredSize = mapper.resize(size);
            Object[] newKeys = new Object[requiredSize];
            byte[] newValues = new byte[requiredSize * 8];
            mapper.reset(newKeys);
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == null) continue;
                int newOffset = mapper.put(keys[i]);
                float oldValue1 = Platform.getFloat(keys, i * 8);
                float oldValue2 = Platform.getFloat(keys, i * 8 + 4);
                Platform.putFloat(newValues, newOffset * 8, oldValue1);
                Platform.putFloat(newValues, newOffset * 8, oldValue2 + 4);
            }
            this.keys = newKeys;
            this.values = newValues;
            this.size = size;
        }
    }

}
