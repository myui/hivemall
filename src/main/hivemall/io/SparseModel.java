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
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.collections.OpenHashMap;

public final class SparseModel extends AbstractPredictionModel {

    private final OpenHashMap<Object, WeightValue> weights;
    private final boolean hasCovar;
    private boolean clockEnabled;

    public SparseModel(int size, boolean hasCovar) {
        super();
        this.weights = new OpenHashMap<Object, WeightValue>(size);
        this.hasCovar = hasCovar;
        this.clockEnabled = false;
    }

    @Override
    public boolean hasCovariance() {
        return hasCovar;
    }

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
    public <T extends WeightValue> T get(Object feature) {
        return (T) weights.get(feature);
    }

    @Override
    public <T extends WeightValue> void set(Object feature, T value) {
        assert (feature != null);
        assert (value != null);

        if(clockEnabled && value.isTouched()) {
            WeightValue old = weights.get(feature);
            if(old != null) {
                short newclock = (short) (old.getClock() + value.getClock());
                assert (newclock >= 0) : newclock;
                value.setClock(newclock);
                byte newDelta = (byte) (old.getDeltaUpdates() + 1);
                assert (newDelta > 0) : newclock;
                value.setDeltaUpdates(newDelta);
            }
        }
        weights.put(feature, value);

        onUpdate(feature, value);
    }

    @Override
    public float getWeight(Object feature) {
        WeightValue v = weights.get(feature);
        return v == null ? 0.f : v.value;
    }

    @Override
    public float getCovariance(Object feature) {
        WeightValueWithCovar v = (WeightValueWithCovar) weights.get(feature);
        return v == null ? 1.f : v.covariance;
    }

    @Override
    public void _set(Object feature, float weight, short clock) {
        WeightValue w = weights.get(feature);
        if(w == null) {
            // throw new IllegalStateException("Previous weight not found");
            w = new WeightValue(weight);
            w.setClock(clock);
            weights.put(feature, w);
        } else {
            w.set(weight);
            w.setClock(clock);
            w.setDeltaUpdates(BYTE0);
        }
        numMixed++;
    }

    @Override
    public void _set(Object feature, float weight, float covar, short clock) {
        WeightValue w = weights.get(feature);
        if(w == null) {
            // throw new IllegalStateException("Previous weight not found");
            w = new WeightValueWithCovar(weight, covar);
            w.setClock(clock);
            weights.put(feature, w);
        } else {
            w.set(weight);
            w.setCovariance(covar);
            w.setClock(clock);
            w.setDeltaUpdates(BYTE0);
        }
        numMixed++;
    }

    @Override
    public int size() {
        return weights.size();
    }

    @Override
    public boolean contains(Object feature) {
        return weights.containsKey(feature);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V extends WeightValue> IMapIterator<K, V> entries() {
        return (IMapIterator<K, V>) weights.entries();
    }

}
