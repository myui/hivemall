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
package hivemall.model;

import hivemall.model.WeightValue.WeightValueParamsF1;
import hivemall.model.WeightValue.WeightValueParamsF2;
import hivemall.model.WeightValue.WeightValueWithCovar;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Copyable;
import hivemall.utils.math.MathUtils;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class DenseModel extends AbstractPredictionModel {
    private static final Log logger = LogFactory.getLog(DenseModel.class);

    private int size;
    private float[] weights;
    private float[] covars;

    // optional values for adagrad
    private float[] sum_of_squared_gradients;
    // optional value for adadelta
    private float[] sum_of_squared_delta_x;
    // optional value for adagrad+rda
    private float[] sum_of_gradients;

    // optional value for MIX
    private short[] clocks;
    private byte[] deltaUpdates;

    public DenseModel(int ndims) {
        this(ndims, false);
    }

    public DenseModel(int ndims, boolean withCovar) {
        super();
        int size = ndims + 1;
        this.size = size;
        this.weights = new float[size];
        if (withCovar) {
            float[] covars = new float[size];
            Arrays.fill(covars, 1f);
            this.covars = covars;
        } else {
            this.covars = null;
        }
        this.sum_of_squared_gradients = null;
        this.sum_of_squared_delta_x = null;
        this.sum_of_gradients = null;
        this.clocks = null;
        this.deltaUpdates = null;
    }

    @Override
    protected boolean isDenseModel() {
        return true;
    }

    @Override
    public boolean hasCovariance() {
        return covars != null;
    }

    @Override
    public void configureParams(boolean sum_of_squared_gradients, boolean sum_of_squared_delta_x,
            boolean sum_of_gradients) {
        if (sum_of_squared_gradients) {
            this.sum_of_squared_gradients = new float[size];
        }
        if (sum_of_squared_delta_x) {
            this.sum_of_squared_delta_x = new float[size];
        }
        if (sum_of_gradients) {
            this.sum_of_gradients = new float[size];
        }
    }

    @Override
    public void configureClock() {
        if (clocks == null) {
            this.clocks = new short[size];
            this.deltaUpdates = new byte[size];
        }
    }

    @Override
    public boolean hasClock() {
        return clocks != null;
    }

    @Override
    public void resetDeltaUpdates(int feature) {
        deltaUpdates[feature] = 0;
    }

    private void ensureCapacity(final int index) {
        if (index >= size) {
            int bits = MathUtils.bitsRequired(index);
            int newSize = (1 << bits) + 1;
            int oldSize = size;
            logger.info("Expands internal array size from " + oldSize + " to " + newSize + " ("
                    + bits + " bits)");
            this.size = newSize;
            this.weights = Arrays.copyOf(weights, newSize);
            if (covars != null) {
                this.covars = Arrays.copyOf(covars, newSize);
                Arrays.fill(covars, oldSize, newSize, 1.f);
            }
            if (sum_of_squared_gradients != null) {
                this.sum_of_squared_gradients = Arrays.copyOf(sum_of_squared_gradients, newSize);
            }
            if (sum_of_squared_delta_x != null) {
                this.sum_of_squared_delta_x = Arrays.copyOf(sum_of_squared_delta_x, newSize);
            }
            if (sum_of_gradients != null) {
                this.sum_of_gradients = Arrays.copyOf(sum_of_gradients, newSize);
            }
            if (clocks != null) {
                this.clocks = Arrays.copyOf(clocks, newSize);
                this.deltaUpdates = Arrays.copyOf(deltaUpdates, newSize);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IWeightValue> T get(Object feature) {
        final int i = HiveUtils.parseInt(feature);
        if (i >= size) {
            return null;
        }
        if (sum_of_squared_gradients != null) {
            if (sum_of_squared_delta_x != null) {
                return (T) new WeightValueParamsF2(weights[i], sum_of_squared_gradients[i],
                    sum_of_squared_delta_x[i]);
            } else if (sum_of_gradients != null) {
                return (T) new WeightValueParamsF2(weights[i], sum_of_squared_gradients[i],
                    sum_of_gradients[i]);
            } else {
                return (T) new WeightValueParamsF1(weights[i], sum_of_squared_gradients[i]);
            }
        } else if (covars != null) {
            return (T) new WeightValueWithCovar(weights[i], covars[i]);
        } else {
            return (T) new WeightValue(weights[i]);
        }
    }

    @Override
    public <T extends IWeightValue> void set(Object feature, T value) {
        int i = HiveUtils.parseInt(feature);
        ensureCapacity(i);
        float weight = value.get();
        weights[i] = weight;
        float covar = 1.f;
        boolean hasCovar = value.hasCovariance();
        if (hasCovar) {
            covar = value.getCovariance();
            covars[i] = covar;
        }
        if (sum_of_squared_gradients != null) {
            sum_of_squared_gradients[i] = value.getSumOfSquaredGradients();
        }
        if (sum_of_squared_delta_x != null) {
            sum_of_squared_delta_x[i] = value.getSumOfSquaredDeltaX();
        }
        if (sum_of_gradients != null) {
            sum_of_gradients[i] = value.getSumOfGradients();
        }
        short clock = 0;
        int delta = 0;
        if (clocks != null && value.isTouched()) {
            clock = (short) (clocks[i] + 1);
            clocks[i] = clock;
            delta = deltaUpdates[i] + 1;
            assert (delta > 0) : delta;
            deltaUpdates[i] = (byte) delta;
        }

        onUpdate(i, weight, covar, clock, delta, hasCovar);
    }

    @Override
    public void delete(@Nonnull Object feature) {
        final int i = HiveUtils.parseInt(feature);
        if (i >= size) {
            return;
        }
        weights[i] = 0.f;
        if (covars != null) {
            covars[i] = 1.f;
        }
        if (sum_of_squared_gradients != null) {
            sum_of_squared_gradients[i] = 0.f;
        }
        if (sum_of_squared_delta_x != null) {
            sum_of_squared_delta_x[i] = 0.f;
        }
        if (sum_of_gradients != null) {
            sum_of_gradients[i] = 0.f;
        }
        // avoid clock/delta
    }

    @Override
    public float getWeight(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if (i >= size) {
            return 0f;
        }
        return weights[i];
    }

    @Override
    public float getCovariance(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if (i >= size) {
            return 1f;
        }
        return covars[i];
    }

    @Override
    protected void _set(Object feature, float weight, short clock) {
        int i = ((Integer) feature).intValue();
        ensureCapacity(i);
        weights[i] = weight;
        clocks[i] = clock;
        deltaUpdates[i] = 0;
    }

    @Override
    protected void _set(Object feature, float weight, float covar, short clock) {
        int i = ((Integer) feature).intValue();
        ensureCapacity(i);
        weights[i] = weight;
        covars[i] = covar;
        clocks[i] = clock;
        deltaUpdates[i] = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if (i >= size) {
            return false;
        }
        float w = weights[i];
        return w != 0.f;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V extends IWeightValue> IMapIterator<K, V> entries() {
        return (IMapIterator<K, V>) new Itr();
    }

    private final class Itr implements IMapIterator<Number, IWeightValue> {

        private int cursor;
        private final WeightValueWithCovar tmpWeight;

        private Itr() {
            this.cursor = -1;
            this.tmpWeight = new WeightValueWithCovar();
        }

        @Override
        public boolean hasNext() {
            return cursor < size;
        }

        @Override
        public int next() {
            ++cursor;
            if (!hasNext()) {
                return -1;
            }
            return cursor;
        }

        @Override
        public Integer getKey() {
            return cursor;
        }

        @Override
        public IWeightValue getValue() {
            if (covars == null) {
                float w = weights[cursor];
                WeightValue v = new WeightValue(w);
                v.setTouched(w != 0f);
                return v;
            } else {
                float w = weights[cursor];
                float cov = covars[cursor];
                WeightValueWithCovar v = new WeightValueWithCovar(w, cov);
                v.setTouched(w != 0.f || cov != 1.f);
                return v;
            }
        }

        @Override
        public <T extends Copyable<IWeightValue>> void getValue(T probe) {
            float w = weights[cursor];
            tmpWeight.value = w;
            float cov = 1.f;
            if (covars != null) {
                cov = covars[cursor];
                tmpWeight.setCovariance(cov);
            }
            tmpWeight.setTouched(w != 0.f || cov != 1.f);
            probe.copyFrom(tmpWeight);
        }

    }

}
