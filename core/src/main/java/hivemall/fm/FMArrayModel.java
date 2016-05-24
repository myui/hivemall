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
package hivemall.fm;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public final class FMArrayModel extends FactorizationMachineModel {

    private final int _p;

    // LEARNING PARAMS
    private final float[] _w;
    private final float[][] _V;

    public FMArrayModel(@Nonnull FMHyperParameters params) {
        super(params);
        this._p = params.numFeatures;
        this._w = new float[params.numFeatures + 1];
        this._V = new float[params.numFeatures][params.factor];
    }

    @Override
    protected void initLearningParams() {
        for (int i = 0; i < _p; i++) {
            _V[i] = initV();
        }
    }

    @Override
    protected int getMinIndex() {
        return 1;
    }

    @Override
    protected int getMaxIndex() {
        return _p - 1;
    }

    @Override
    public int getSize() {
        return _p;
    }

    @Override
    public float getW0() {
        return _w[0];
    }

    @Override
    protected void setW0(float nextW0) {
        _w[0] = nextW0;
    }

    @Override
    protected float getW(int i) {
        assert (i >= 1) : i;
        return _w[i];
    }

    @Override
    public float getW(@Nonnull final Feature x) {
        int i = x.getFeatureIndex();
        assert (i >= 0) : i;
        return _w[i];
    }

    @Override
    protected void setW(@Nonnull Feature x, float nextWi) {
        int i = x.getFeatureIndex();
        assert (i >= 0) : i;
        _w[i] = nextWi;
    }

    @Override
    protected float[] getV(int i) {
        if (i < 1 || i > _p) {
            throw new IllegalArgumentException("Index i should be in range [1," + _p + "]: " + i);
        }
        return _V[i - 1];
    }

    @Override
    public float getV(@Nonnull final Feature x, int f) {
        final int i = x.getFeatureIndex();
        if (i < 1 || i > _p) {
            throw new IllegalArgumentException("Index i should be in range [1," + _p + "]: " + i);
        }
        return _V[i - 1][f];
    }

    @Override
    protected void setV(@Nonnull Feature x, int f, float nextVif) {
        final int i = x.getFeatureIndex();
        if (i < 1 || i > _p) {
            throw new IllegalArgumentException("Index i should be in range [1," + _p + "]: " + i);
        }
        _V[i - 1][f] = nextVif;
    }

    @Override
    public void check(@Nonnull Feature[] x) throws HiveException {
        for (Feature e : x) {
            if (e != null && e.getFeatureIndex() < 1) {
                throw new HiveException("Index of x should be greater than or equals to 1: "
                        + Arrays.toString(x));
            }
        }
    }

}
