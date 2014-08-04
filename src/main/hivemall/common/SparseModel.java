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
package hivemall.common;

import hivemall.common.WeightValue.WeightValueWithCovar;
import hivemall.utils.collections.IMapIterator;
import hivemall.utils.collections.OpenHashMap;

public final class SparseModel implements PredictionModel {

    private final OpenHashMap<Object, WeightValue> weights;

    public SparseModel() {
        this(16384);
    }

    public SparseModel(int size) {
        this.weights = new OpenHashMap<Object, WeightValue>(size);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends WeightValue> T get(Object feature) {
        return (T) weights.get(feature);
    }

    @Override
    public <T extends WeightValue> void set(Object feature, T value) {
        weights.put(feature, value);
    }

    @Override
    public float getWeight(Object feature) {
        WeightValue v = weights.get(feature);
        return v == null ? 0.f : v.value;
    }

    @Override
    public float getCovariance(Object feature) {
        WeightValueWithCovar v = (WeightValueWithCovar) weights.get(feature);
        return v == null ? 0.f : v.covariance;
    }

    @Override
    public void setValue(Object feature, float weight) {
        weights.put(feature, new WeightValue(weight));
    }

    @Override
    public void setValue(Object feature, float weight, float covar) {
        weights.put(feature, new WeightValueWithCovar(weight, covar));
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
