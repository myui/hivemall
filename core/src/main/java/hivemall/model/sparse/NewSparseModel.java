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

import hivemall.model.*;
import hivemall.model.Solver.SolverType;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.collections.OpenHashMap;

import javax.annotation.Nonnull;

import hivemall.utils.lang.Copyable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public final class NewSparseModel extends AbstractPredictionModel {
    private static final Log logger = LogFactory.getLog(SparseModel.class);

    private final OpenHashMap<Object, Float> weights;
    private final OpenHashMap<Object, Float> covars;
    private final boolean hasCovar;

    // Implement a solver to update weights
    private final Solver solverImpl;

    private boolean clockEnabled;

    public NewSparseModel(int size, boolean hasCovar) {
        this(size, hasCovar, SolverType.Default, new HashMap<String, String>());
    }

    public NewSparseModel(int size, boolean hasCovar, SolverType solverType, Map<String, String> options) {
        super();
        this.weights = new OpenHashMap<Object, Float>(size);
        this.covars = null; // Not used supported
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
        return (T) new WeightValue(weights.get(feature));
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
            if (weights.containsKey(feature)) {
                final Float weight = weights.get(feature);
                weights.put(feature, solverImpl.computeUpdatedValue(feature, weight, xi, gradient));
            } else {
                weights.put(feature, solverImpl.computeUpdatedValue(feature, 0.f, xi, gradient));
            }
        }
        solverImpl.proceedStep();
    }

    @Override
    public void delete(@Nonnull Object feature) {
        weights.remove(feature);
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
        Float v = weights.get(feature);
        return v == null ? 0.f : v;
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
        return weights.size();
    }

    @Override
    public boolean contains(final Object feature) {
        return weights.containsKey(feature);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V extends IWeightValue> IMapIterator<K, V> entries() {
        return (IMapIterator<K, V>) new Itr();
    }

    private final class Itr implements IMapIterator<Number, IWeightValue> {

        private final WeightValue.WeightValueWithCovar tmpWeight;
        private final IMapIterator<Object, Float> weightIter;

        private Itr() {
            this.tmpWeight = new WeightValue.WeightValueWithCovar();
            this.weightIter = weights.entries();
        }

        @Override
        public boolean hasNext() {
            return weightIter.hasNext();
        }

        @Override
        public int next() {
            return weightIter.next();
        }

        @Override
        public Integer getKey() {
            return (Integer) weightIter.getKey();
        }

        @Override
        public IWeightValue getValue() {
            assert(!hasCovar);
            float w = weightIter.getValue();
            WeightValue v = new WeightValue(w);
            v.setTouched(w != 0f);
            return v;
        }

        @Override
        public <T extends Copyable<IWeightValue>> void getValue(T probe) {
            assert(!hasCovar);
            float w = weightIter.getValue();
            tmpWeight.set(w);
            tmpWeight.setTouched(w != 0.f);
            probe.copyFrom(tmpWeight);
        }

    }

}
