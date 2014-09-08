/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.io;

import hivemall.io.WeightValue.WeightValueWithCovar;
import hivemall.io.WeightValue.WeightValueWithGt;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Copyable;
import hivemall.utils.math.MathUtils;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class DenseModel extends AbstractPredictionModel {
    private static final Log logger = LogFactory.getLog(DenseModel.class);

    private int size;
    private float[] weights;
    private float[] covars;

    // optional values
    private float[] sum_of_squared_gradients;
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
        if(withCovar) {
            float[] covars = new float[size];
            Arrays.fill(covars, 1f);
            this.covars = covars;
        } else {
            this.covars = null;
        }
        this.sum_of_squared_gradients = null;
        this.clocks = null;
        this.deltaUpdates = null;
    }

    @Override
    public boolean hasCovariance() {
        return covars != null;
    }

    @Override
    public void configurParams(boolean sumOfSquaredGradients) {
        if(sumOfSquaredGradients) {
            this.sum_of_squared_gradients = new float[size];
        }
    }

    @Override
    public void configureClock() {
        if(clocks == null) {
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
        if(index >= size) {
            int bits = MathUtils.bitsRequired(index);
            int newSize = (1 << bits) + 1;
            int oldSize = size;
            logger.info("Expands internal array size from " + oldSize + " to " + newSize + " ("
                    + bits + " bits)");
            this.size = newSize;
            this.weights = Arrays.copyOf(weights, newSize);
            if(covars != null) {
                this.covars = Arrays.copyOf(covars, newSize);
                Arrays.fill(covars, oldSize, newSize, 1f);
            }
            if(sum_of_squared_gradients != null) {
                this.sum_of_squared_gradients = Arrays.copyOf(sum_of_squared_gradients, newSize);
            }
            if(clocks != null) {
                this.clocks = Arrays.copyOf(clocks, newSize);
                this.deltaUpdates = Arrays.copyOf(deltaUpdates, newSize);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IWeightValue> T get(Object feature) {
        final int i = HiveUtils.parseInt(feature);
        if(i >= size) {
            return null;
        }
        if(sum_of_squared_gradients != null) {
            return (T) new WeightValueWithGt(weights[i], sum_of_squared_gradients[i]);
        } else if(covars != null) {
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
        if(value.hasCovariance()) {
            covar = value.getCovariance();
            covars[i] = covar;
        }
        if(sum_of_squared_gradients != null) {
            sum_of_squared_gradients[i] = value.getSumOfSquaredGradients();
        }
        short clock = 0;
        int delta = 0;       
        if(clocks != null && value.isTouched()) {
            clock = (short) (clocks[i] + 1);
            clocks[i] = clock;
            delta = deltaUpdates[i] + 1;
            assert (delta > 0) : delta;
            deltaUpdates[i] = (byte) delta;
        }

        onUpdate(i, weight, covar, clock, delta);
    }

    @Override
    public float getWeight(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if(i >= size) {
            return 0f;
        }
        return weights[i];
    }

    @Override
    public float getCovariance(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if(i >= size) {
            return 1f;
        }
        return covars[i];
    }

    @Override
    public void _set(Object feature, float weight, short clock) {
        int i = HiveUtils.parseInt(feature);
        ensureCapacity(i);
        weights[i] = weight;
        clocks[i] = clock;
        deltaUpdates[i] = 0;
        numMixed++;
    }

    @Override
    public void _set(Object feature, float weight, float covar, short clock) {
        int i = HiveUtils.parseInt(feature);
        ensureCapacity(i);
        weights[i] = weight;
        covars[i] = covar;
        clocks[i] = clock;
        deltaUpdates[i] = 0;
        numMixed++;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if(i >= size) {
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
            if(!hasNext()) {
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
            if(covars == null) {
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
            if(covars != null) {
                cov = covars[cursor];
                tmpWeight.setCovariance(cov);
            }
            tmpWeight.setTouched(w != 0.f || cov != 1.f);
            probe.copyFrom(tmpWeight);
        }

    }

}
