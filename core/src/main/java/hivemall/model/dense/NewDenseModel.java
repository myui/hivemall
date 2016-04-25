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

import hivemall.model.*;
import hivemall.model.Solver.SolverType;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Copyable;
import hivemall.utils.math.MathUtils;
import hivemall.utils.unsafe.Platform;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class NewDenseModel extends AbstractPredictionModel {
    private static final Log logger = LogFactory.getLog(DenseModel.class);

    private int size;
    // If `withCovar` enabled, store a sequence of a pair (weight, covar)
    private byte[] model;
    private final int elementSize;

    // Implement a solver to update weights
    private final Solver solverImpl;

    // optional value for MIX
    // TODO: Move these to more optimal classes
    private short[] clocks;
    private byte[] deltaUpdates;

    public NewDenseModel(int ndims) {
        this(ndims, false);
    }

    public NewDenseModel(int ndims, boolean withCovar) {
        this(ndims, withCovar, SolverType.Default, new HashMap<String, String>());
    }

    public NewDenseModel(int ndims, boolean withCovar, SolverType solverType, Map<String, String> options) {
        super();
        int size = ndims + 1;
        int elementSize = 4;
        if (withCovar) {
            elementSize *= 2; // covar co-located with weight
        }
        this.size = size;
        this.elementSize = elementSize;
        this.model = new byte[elementSize * size];
        this.solverImpl = DenseSolverFactory.create(solverType, ndims, options);
        this.clocks = null;
        this.deltaUpdates = null;
    }

    @Override
    protected boolean isDenseModel() {
        return true;
    }

    @Override
    public boolean hasCovariance() {
        return this.elementSize == 8;
    }

    @Override
    public void configureParams(boolean v1, boolean v2, boolean v3) {}

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

            this.model = Arrays.copyOf(model, newSize * elementSize);
            if (hasCovariance()) {
                for (int i = oldSize; i < newSize; i++) {
                    Platform.putFloat(this.model, i * elementSize + 4, 1.f);
                }
            }
            if(clocks != null) {
                this.clocks = Arrays.copyOf(clocks, newSize);
                this.deltaUpdates = Arrays.copyOf(deltaUpdates, newSize);
            }
        }
    }

    private float getWeight(int index) {
        return Platform.getFloat(this.model, index * elementSize);
    }
    private void putWeight(int index, float value) {
        Platform.putFloat(this.model, index * elementSize, value);
    }
    private float getCovar(int index) {
        return hasCovariance()? Platform.getFloat(this.model, index * elementSize + 1) : 1.f;
    }
    private void putCovar(int index, float value) {
        if (hasCovariance()) {
            Platform.putFloat(this.model, index * elementSize + 1, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IWeightValue> T get(Object feature) {
        final int i = HiveUtils.parseInt(feature);
        if(i >= size) {
            return null;
        }
        return (T) new WeightValue(getWeight(i));
    }

    @Override
    public <T extends IWeightValue> void set(Object feature, T value) {
        int i = HiveUtils.parseInt(feature);
        ensureCapacity(i);
        float weight = value.get();
        putWeight(i, weight);
        float covar = 1.f;
        boolean hasCovar = value.hasCovariance();
        if(hasCovar) {
            covar = value.getCovariance();
            putCovar(i, covar);
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

        onUpdate(i, weight, covar, clock, delta, hasCovar);
    }

    @Override
    public void updateWeight(@Nonnull FeatureValue[] features, float gradient) {
        for(FeatureValue f : features) {
            int i = HiveUtils.parseInt(f.getFeature());
            ensureCapacity(i);
            float oldWeight = getWeight(i);
            float weight = solverImpl.computeUpdatedValue(
                    f.getFeature(), oldWeight, f.getValueAsFloat(), gradient);
            putWeight(i, weight);
        }
        solverImpl.proceedStep();
    }

    @Override
    public void delete(@Nonnull Object feature) {
        final int i = HiveUtils.parseInt(feature);
        if(i >= size) {
            return;
        }
        putWeight(i, 0.f);
        putCovar(i, 1.f);
        // avoid clock/delta
    }

    @Override
    public float getWeight(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if(i >= size) {
            return 0f;
        }
        return getWeight(i);
    }

    @Override
    public float getCovariance(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if(i >= size) {
            return 1f;
        }
        return getCovar(i);
    }

    @Override
    protected void _set(Object feature, float weight, short clock) {
        int i = ((Integer) feature).intValue();
        ensureCapacity(i);
        putWeight(i, weight);
        clocks[i] = clock;
        deltaUpdates[i] = 0;
    }

    @Override
    protected void _set(Object feature, float weight, float covar, short clock) {
        int i = ((Integer) feature).intValue();
        ensureCapacity(i);
        putWeight(i, weight);
        putCovar(i, covar);
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
        if(i >= size) {
            return false;
        }
        float w = getWeight(i);
        return w != 0.f;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V extends IWeightValue> IMapIterator<K, V> entries() {
        return (IMapIterator<K, V>) new Itr();
    }

    private final class Itr implements IMapIterator<Number, IWeightValue> {

        private int cursor;
        private final WeightValue.WeightValueWithCovar tmpWeight;

        private Itr() {
            this.cursor = -1;
            this.tmpWeight = new WeightValue.WeightValueWithCovar();
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
            if(!hasCovariance()) {
                float w = getWeight(cursor);
                WeightValue v = new WeightValue(w);
                v.setTouched(w != 0f);
                return v;
            } else {
                float w = getWeight(cursor);
                float cov = getCovar(cursor);
                WeightValue.WeightValueWithCovar v = new WeightValue.WeightValueWithCovar(w, cov);
                v.setTouched(w != 0.f || cov != 1.f);
                return v;
            }
        }

        @Override
        public <T extends Copyable<IWeightValue>> void getValue(T probe) {
            float w = getWeight(cursor);
            tmpWeight.set(w);
            float cov = 1.f;
            if(hasCovariance()) {
                cov = getCovar(cursor);
                tmpWeight.setCovariance(cov);
            }
            tmpWeight.setTouched(w != 0.f || cov != 1.f);
            probe.copyFrom(tmpWeight);
        }

    }

}
