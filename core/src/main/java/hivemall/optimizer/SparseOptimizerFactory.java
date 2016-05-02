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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import hivemall.optimizer.Optimizer.OptimizerBase;
import hivemall.model.IWeightValue;
import hivemall.model.WeightValue;
import hivemall.utils.collections.OpenHashMap;

public final class SparseOptimizerFactory {
    private static final Log logger = LogFactory.getLog(SparseOptimizerFactory.class);

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

        private final OpenHashMap<Object, IWeightValue> auxWeights;

        public AdaDelta(int size, Map<String, String> options) {
            super(options);
            this.auxWeights = new OpenHashMap<Object, IWeightValue>(size);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            IWeightValue auxWeight;
            if(auxWeights.containsKey(feature)) {
                auxWeight = auxWeights.get(feature);
                auxWeight.set(weight);
            } else {
                auxWeight = new WeightValue.WeightValueParamsF2(weight, 0.f, 0.f);
                auxWeights.put(feature, auxWeight);
            }
            computeUpdateValue(auxWeight, gradient);
            return auxWeight.get();
        }

    }

    @NotThreadSafe
    static final class AdaGrad extends Optimizer.AdaGrad {

        private final OpenHashMap<Object, IWeightValue> auxWeights;

        public AdaGrad(int size, Map<String, String> options) {
            super(options);
            this.auxWeights = new OpenHashMap<Object, IWeightValue>(size);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            IWeightValue auxWeight;
            if(auxWeights.containsKey(feature)) {
                auxWeight = auxWeights.get(feature);
                auxWeight.set(weight);
            } else {
                auxWeight = new WeightValue.WeightValueParamsF2(weight, 0.f, 0.f);
                auxWeights.put(feature, auxWeight);
            }
            computeUpdateValue(auxWeight, gradient);
            return auxWeight.get();
        }

    }

    @NotThreadSafe
    static final class Adam extends Optimizer.Adam {

        private final OpenHashMap<Object, IWeightValue> auxWeights;

        public Adam(int size, Map<String, String> options) {
            super(options);
            this.auxWeights = new OpenHashMap<Object, IWeightValue>(size);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            IWeightValue auxWeight;
            if(auxWeights.containsKey(feature)) {
                auxWeight = auxWeights.get(feature);
                auxWeight.set(weight);
            } else {
                auxWeight = new WeightValue.WeightValueParamsF2(weight, 0.f, 0.f);
                auxWeights.put(feature, auxWeight);
            }
            computeUpdateValue(auxWeight, gradient);
            return auxWeight.get();
        }

    }

    @NotThreadSafe
    static final class RDA extends Optimizer.RDA {

        private final OpenHashMap<Object, IWeightValue> auxWeights;

        public RDA(int size, OptimizerBase optimizerImpl, Map<String, String> options) {
            super(optimizerImpl, options);
            this.auxWeights = new OpenHashMap<Object, IWeightValue>(size);
        }

        @Override
        public float computeUpdatedValue(@Nonnull Object feature, float weight, float gradient) {
            IWeightValue auxWeight;
            if(auxWeights.containsKey(feature)) {
                auxWeight = auxWeights.get(feature);
                auxWeight.set(weight);
            } else {
                auxWeight = new WeightValue.WeightValueParamsF2(weight, 0.f, 0.f);
                auxWeights.put(feature, auxWeight);
            }
            computeUpdateValue(auxWeight, gradient);
            return auxWeight.get();
        }

    }

}
