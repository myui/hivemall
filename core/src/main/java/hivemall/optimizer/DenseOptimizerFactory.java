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
package hivemall.optimizer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import hivemall.optimizer.Optimizer.OptimizerBase;
import hivemall.model.IWeightValue;
import hivemall.model.WeightValue;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.math.MathUtils;

public final class DenseOptimizerFactory {
    private static final Log logger = LogFactory.getLog(DenseOptimizerFactory.class);

    @Nonnull
    public static Optimizer create(int ndims, @Nonnull Map<String, String> options) {
        final String optimizerName = options.get("optimizer");
        if(optimizerName != null) {
            OptimizerBase optimizerImpl;
            if(optimizerName.toLowerCase().equals("sgd")) {
                optimizerImpl = new Optimizer.SGD(options);
            } else if(optimizerName.toLowerCase().equals("adadelta")) {
                optimizerImpl = new AdaDelta(ndims, options);
            } else if(optimizerName.toLowerCase().equals("adagrad")) {
                optimizerImpl = new AdaGrad(ndims, options);
            } else if(optimizerName.toLowerCase().equals("adam")) {
                optimizerImpl = new Adam(ndims, options);
            } else {
                throw new IllegalArgumentException("Unsupported optimizer name: " + optimizerName);
            }

            logger.info("set " + optimizerImpl.getClass().getSimpleName()
                    + " as an optimizer: " + options);

            // If a regularization type is "RDA", wrap the optimizer with `Optimizer#RDA`.
            if(options.get("regularization") != null
                    && options.get("regularization").toLowerCase().equals("rda")) {
                optimizerImpl = new RDA(ndims, optimizerImpl, options);
            }

            return optimizerImpl;
        }
        throw new IllegalArgumentException("`optimizer` not defined");
    }

    @NotThreadSafe
    static final class AdaDelta extends Optimizer.AdaDelta {

        private final IWeightValue weightValueReused;

        private float[] sum_of_squared_gradients;
        private float[] sum_of_squared_delta_x;

        public AdaDelta(int ndims, Map<String, String> options) {
            super(options);
            this.weightValueReused = new WeightValue.WeightValueParamsF2(0.f, 0.f, 0.f);
            this.sum_of_squared_gradients = new float[ndims];
            this.sum_of_squared_delta_x = new float[ndims];
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setSumOfSquaredGradients(sum_of_squared_gradients[i]);
            weightValueReused.setSumOfSquaredDeltaX(sum_of_squared_delta_x[i]);
            computeUpdateValue(weightValueReused, gradient);
            sum_of_squared_gradients[i] = weightValueReused.getSumOfSquaredGradients();
            sum_of_squared_delta_x[i] = weightValueReused.getSumOfSquaredDeltaX();
            return weightValueReused.get();
        }

        private void ensureCapacity(final int index) {
            if(index >= sum_of_squared_gradients.length) {
                int bits = MathUtils.bitsRequired(index);
                int newSize = (1 << bits) + 1;
                this.sum_of_squared_gradients = Arrays.copyOf(sum_of_squared_gradients, newSize);
                this.sum_of_squared_delta_x = Arrays.copyOf(sum_of_squared_delta_x, newSize);
            }
        }

    }

    @NotThreadSafe
    static final class AdaGrad extends Optimizer.AdaGrad {

        private final IWeightValue weightValueReused;

        private float[] sum_of_squared_gradients;

        public AdaGrad(int ndims, Map<String, String> options) {
            super(options);
            this.weightValueReused = new WeightValue.WeightValueParamsF1(0.f, 0.f);
            this.sum_of_squared_gradients = new float[ndims];
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setSumOfSquaredGradients(sum_of_squared_gradients[i]);
            computeUpdateValue(weightValueReused, gradient);
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
    static final class Adam extends Optimizer.Adam {

        private final IWeightValue weightValueReused;

        private float[] val_m;
        private float[] val_v;

        public Adam(int ndims, Map<String, String> options) {
            super(options);
            this.weightValueReused = new WeightValue.WeightValueParamsF2(0.f, 0.f, 0.f);
            this.val_m = new float[ndims];
            this.val_v = new float[ndims];
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setM(val_m[i]);
            weightValueReused.setV(val_v[i]);
            computeUpdateValue(weightValueReused, gradient);
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

    @NotThreadSafe
    static final class RDA extends Optimizer.RDA {

        private final IWeightValue weightValueReused;

        private float[] sum_of_gradients;

        public RDA(int ndims, final OptimizerBase optimizerImpl, Map<String, String> options) {
            super(optimizerImpl, options);
            this.weightValueReused = new WeightValue.WeightValueParamsF3(0.f, 0.f, 0.f, 0.f);
            this.sum_of_gradients = new float[ndims];
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            int i = HiveUtils.parseInt(feature);
            ensureCapacity(i);
            weightValueReused.set(weight);
            weightValueReused.setSumOfGradients(sum_of_gradients[i]);
            computeUpdateValue(weightValueReused, gradient);
            sum_of_gradients[i] = weightValueReused.getSumOfGradients();
            return weightValueReused.get();
        }

        private void ensureCapacity(final int index) {
            if(index >= sum_of_gradients.length) {
                int bits = MathUtils.bitsRequired(index);
                int newSize = (1 << bits) + 1;
                this.sum_of_gradients = Arrays.copyOf(sum_of_gradients, newSize);
            }
        }

    }

}
