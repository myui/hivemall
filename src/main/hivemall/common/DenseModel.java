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
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Copyable;

import java.util.Arrays;

public final class DenseModel implements PredictionModel {

    private final int size;
    private final float[] weights;
    private final float[] covars;

    public DenseModel(int ndims) {
        this(ndims, false);
    }

    public DenseModel(int ndims, boolean withCovar) {
        int size = ndims + 1;
        this.size = size;
        this.weights = new float[size];
        if(withCovar) {
            float[] covars = new float[size];
            Arrays.fill(covars, 1.f);
            this.covars = covars;
        } else {
            this.covars = null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends WeightValue> T get(Object feature) {
        int i = HiveUtils.parseInt(feature);
        if(covars == null) {
            return (T) new WeightValue(weights[i]);
        } else {
            return (T) new WeightValueWithCovar(weights[i], covars[i]);
        }
    }

    @Override
    public <T extends WeightValue> void set(Object feature, T value) {
        int i = HiveUtils.parseInt(feature);
        float weight = value.get();
        weights[i] = weight;
        if(value.hasCovariance()) {
            float covar = value.getCovariance();
            covars[i] = covar;
        }
    }

    @Override
    public float getWeight(Object feature) {
        int i = HiveUtils.parseInt(feature);
        return weights[i];
    }

    @Override
    public float getCovariance(Object feature) {
        int i = HiveUtils.parseInt(feature);
        return covars[i];
    }

    @Override
    public void setValue(Object feature, float weight) {
        int i = HiveUtils.parseInt(feature);
        weights[i] = weight;
    }

    @Override
    public void setValue(Object feature, float weight, float covar) {
        int i = HiveUtils.parseInt(feature);
        weights[i] = weight;
        covars[i] = covar;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(Object feature) {
        int i = HiveUtils.parseInt(feature);
        float w = weights[i];
        return w != 0.f;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V extends WeightValue> IMapIterator<K, V> entries() {
        return (IMapIterator<K, V>) new Itr(size);
    }

    private final class Itr implements IMapIterator<Number, WeightValue> {

        private int cursor;
        private final WeightValue tmpWeight;

        Itr(int size) {
            this.cursor = -1;
            this.tmpWeight = (covars == null) ? new WeightValue() : new WeightValueWithCovar();
        }

        @Override
        public boolean hasNext() {
            return cursor != size;
        }

        @Override
        public int next() {
            if(!hasNext()) {
                return -1;
            }
            cursor++;
            return cursor;
        }

        @Override
        public Integer getKey() {
            return cursor;
        }

        @Override
        public WeightValue getValue() {
            if(covars == null) {
                float w = weights[cursor];
                return new WeightValue(w);
            } else {
                float w = weights[cursor];
                float cov = covars[cursor];
                return new WeightValueWithCovar(w, cov);
            }
        }

        @Override
        public <T extends Copyable<WeightValue>> void getValue(T probe) {
            tmpWeight.value = weights[cursor];
            if(covars != null) {
                ((WeightValueWithCovar) tmpWeight).covariance = covars[cursor];
            }
            probe.copyFrom(tmpWeight);
        }

    }

}
