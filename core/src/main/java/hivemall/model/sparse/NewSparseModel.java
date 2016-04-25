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


import javax.annotation.Nonnull;

import hivemall.utils.hadoop.HiveUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import hivemall.utils.lang.Copyable;
import hivemall.utils.unsafe.Platform;
import hivemall.utils.unsafe.UnsafeOpenHashMap;
import hivemall.model.*;
import hivemall.model.Solver.SolverType;
import hivemall.utils.collections.IMapIterator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class NewSparseModel extends AbstractPredictionModel {
    private static final Log logger = LogFactory.getLog(SparseModel.class);

    private int size;
    private int actualSize;

    private Object[] keys;
    private byte[] model;

    private UnsafeOpenHashMap<Object> mapper;

    private final boolean hasCovar;

    // Implement a solver to update weights
    private final Solver solverImpl;

    private boolean clockEnabled;

    public NewSparseModel(int size, boolean hasCovar) {
        this(size, hasCovar, SolverType.Default, new HashMap<String, String>());
    }

    public NewSparseModel(int size, boolean hasCovar, SolverType solverType, Map<String, String> options) {
        super();
        this.size = size;
        // this.covars = null; // Not used supported
        this.mapper = new UnsafeOpenHashMap<Object>();
        int requiredSize = mapper.resize(size);
        this.actualSize = requiredSize;
        this.keys = new Object[requiredSize];
        this.model = new byte[requiredSize * 4];
        this.mapper.reset(keys);
        this.hasCovar = hasCovar;
        this.solverImpl = SparseSolverFactory.create(solverType, size, options);
        this.clockEnabled = false;
    }

    @Override
    protected boolean isDenseModel() {
        return false;
    }

    @Override
    public boolean hasCovariance() {
        return hasCovar;
    }

    @Override
    public void configureParams(boolean sum_of_squared_gradients, boolean sum_of_squared_delta_x, boolean sum_of_gradients) {}

    @Override
    public void configureClock() {
        this.clockEnabled = true;
    }

    @Override
    public boolean hasClock() {
        return clockEnabled;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IWeightValue> T get(final Object feature) {
        return (T) new WeightValue(getWeight(feature));
    }

    @Override
    public <T extends IWeightValue> void set(final Object feature, final T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateWeight(@Nonnull FeatureValue[] features, float gradient) {
        for(FeatureValue f : features) {
            Object feature = f.getFeature();
            float xi = f.getValueAsFloat();
            int offset = mapper.get(feature);
            if (offset == -1) {
                offset = mapper.put(feature);
                if (offset == -1) {
                    // Make space bigger
                    reserveInternal(size * 2);
                    offset = mapper.put(feature);
                }
            }
            float oldWeight = Platform.getFloat(model, Platform.BYTE_ARRAY_OFFSET + offset * 4);
            float newWeight = solverImpl.computeUpdatedValue(feature, oldWeight, xi, gradient);
            Platform.putFloat(model, Platform.BYTE_ARRAY_OFFSET + offset * 4, newWeight);
        }
        solverImpl.proceedStep();
    }

    private void reserveInternal(int size) {
        int requiredSize = mapper.resize(size);
        assert(actualSize < requiredSize);
        Object[] newKeys = new Object[requiredSize];
        byte[] newValues = new byte[requiredSize * 4];
        mapper.reset(newKeys);
        for (int i = 0; i < actualSize; i++) {
            if (keys[i] == null) continue;
            int newOffset = mapper.put(keys[i]);
            float oldValue = Platform.getFloat(model, Platform.BYTE_ARRAY_OFFSET + i * 4);
            Platform.putFloat(newValues, Platform.BYTE_ARRAY_OFFSET + newOffset * 4, oldValue);
        }
        this.keys = newKeys;
        this.model = newValues;
        this.size = size;
        this.actualSize = requiredSize;
    }

    @Override
    public void delete(@Nonnull Object feature) {
        mapper.remove(feature);
    }

    private IWeightValue wrapIfRequired(final IWeightValue value) {
        final IWeightValue wrapper;
        if(clockEnabled) {
            switch(value.getType()) {
                case NoParams:
                    wrapper = new WeightValueWithClock(value);
                    break;
                case ParamsCovar:
                    wrapper = new WeightValueWithClock.WeightValueWithCovarClock(value);
                    break;
                case ParamsF1:
                    wrapper = new WeightValueWithClock.WeightValueParamsF1Clock(value);
                    break;
                case ParamsF2:
                    wrapper = new WeightValueWithClock.WeightValueParamsF2Clock(value);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value type: " + value.getType());
            }
        } else {
            wrapper = value;
        }
        return wrapper;
    }

    @Override
    public float getWeight(final Object feature) {
        int offset = mapper.get(feature);
        return offset == -1? 0.f : Platform.getFloat(model, Platform.BYTE_ARRAY_OFFSET + offset * 4);
    }

    @Override
    public float getCovariance(final Object feature) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _set(final Object feature, final float weight, final short clock) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void _set(final Object feature, final float weight, final float covar, final short clock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return mapper.size();
    }

    @Override
    public boolean contains(final Object feature) {
        return mapper.containsKey(feature);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V extends IWeightValue> IMapIterator<K, V> entries() {
        return (IMapIterator<K, V>) new Itr();
    }

    private final class Itr implements IMapIterator<Number, IWeightValue> {

        private Object curKey;
        private int curOffset;
        private final WeightValue.WeightValueWithCovar tmpWeight;
        private final Iterator<Object> keyIter;

        private Itr() {
            this.tmpWeight = new WeightValue.WeightValueWithCovar();
            this.keyIter = mapper.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return keyIter.hasNext();
        }

        @Override
        public int next() {
            try {
                curKey = keyIter.next();
            } catch(Exception e) {
                return -1;
            }
            curOffset = mapper.get(curKey);
            return curOffset;
        }

        @Override
        public Integer getKey() {
            return HiveUtils.parseInt(curKey);
        }

        @Override
        public IWeightValue getValue() {
            assert(!hasCovar);
            float w = Platform.getFloat(model, Platform.BYTE_ARRAY_OFFSET + curOffset * 4);
            WeightValue v = new WeightValue(w);
            v.setTouched(w != 0f);
            return v;
        }

        @Override
        public <T extends Copyable<IWeightValue>> void getValue(T probe) {
            assert(!hasCovar);
            float w = Platform.getFloat(model, Platform.BYTE_ARRAY_OFFSET + curOffset * 4);
            tmpWeight.set(w);
            tmpWeight.setTouched(w != 0.f);
            probe.copyFrom(tmpWeight);
        }

    }

}
