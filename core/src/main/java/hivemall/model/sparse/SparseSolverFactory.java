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
import hivemall.utils.collections.OpenHashMap;

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

        private final OpenHashMap<Object, Float> sum_of_squared_gradients;

        // Reused to use `AdaGradSolver.computeUpdateValue`
        private WeightValue.WeightValueParamsF1 weightValueReused;

        public AdaGradSolver(int size, Map<String, String> solverOptions) {
            super(solverOptions);
            sum_of_squared_gradients = new OpenHashMap<Object, Float>(size);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            float sqg = 0.f;
            if (sum_of_squared_gradients.containsKey(feature)) {
                sqg = sum_of_squared_gradients.get(feature);
            }
            weightValueReused.set(weight);
            weightValueReused.setSumOfSquaredGradients(sqg);
            computeUpdateValue(weightValueReused, xi, gradient);
            sum_of_squared_gradients.put(feature, weightValueReused.getSumOfSquaredGradients());
            return weightValueReused.get();
        }

    }

    private static final class AdamSolver extends Solver.AdamSolver {

        private class PairValue {
            public float val_m = 0.f;
            public float val_v = 0.f;
        }

        private final OpenHashMap<Object, PairValue> values;

        // Reused to use `AdamSolver.computeUpdateValue`
        private WeightValue.WeightValueParamsF1 weightValueReused;

        public AdamSolver(int size, Map<String, String> solverOptions) {
            super(solverOptions);
            values = new OpenHashMap<Object, PairValue>(size);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float xi, float gradient) {
            PairValue pvalue;
            if (values.containsKey(feature)) {
                pvalue  = values.get(feature);
            } else {
                pvalue = new PairValue();
                values.put(feature, pvalue);
            }
            weightValueReused.set(weight);
            weightValueReused.setM(pvalue.val_m);
            weightValueReused.setM(pvalue.val_v);
            computeUpdateValue(weightValueReused, xi, gradient);
            pvalue.val_m = weightValueReused.getM();
            pvalue.val_v = weightValueReused.getV();
            return weightValueReused.get();
        }

    }

}
