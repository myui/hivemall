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

import hivemall.common.EtaEstimator;
import hivemall.utils.math.MathUtils;

import javax.annotation.Nonnull;

public final class FMArrayModel extends FactorizationMachineModel {

    private final int _p;

    // LEARNING PARAMS
    private final float[] _w;
    private final float[][] _V;

    public FMArrayModel(boolean classification, int factor, float lambda0, double sigma, int p, long seed, double minTarget, double maxTarget, @Nonnull EtaEstimator eta) {
        super(classification, factor, lambda0, sigma, seed, minTarget, maxTarget, eta);
        this._p = p;
        this._w = new float[p + 1];
        this._V = new float[p][factor];
    }

    @Override
    protected void initLearningParams() {
        for(int i = 0; i < _p; i++) {
            for(int j = 0; j < _factor; j++) {
                _V[i][j] = (float) MathUtils.gaussian(0.d, _sigma, _rnd);
            }
        }
    }

    @Override
    public int getMinIndex() {
        return 1;
    }

    @Override
    public int getMaxIndex() {
        return _p - 1;
    }

    @Override
    public int getSize() {
        return _p;
    }

    @Override
    public float getW(int i) {
        assert (i >= 0) : i;
        return _w[i];
    }

    @Override
    protected void setW(int i, float nextWi) {
        assert (i >= 0) : i;
        _w[i] = nextWi;
    }

    @Override
    public float[] getV(int i) {
        if(i < 1 || i > _p) {
            throw new IllegalArgumentException("Index i should be in range [1," + _p + "]: " + i);
        }
        return _V[i - 1];
    }

    @Override
    public float getV(int i, int f) {
        if(i < 1 || i > _p) {
            throw new IllegalArgumentException("Index i should be in range [1," + _p + "]: " + i);
        }
        return _V[i - 1][f];
    }

    @Override
    public void setV(int i, int f, float nextVif) {
        if(i < 1 || i > _p) {
            throw new IllegalArgumentException("Index i should be in range [1," + _p + "]: " + i);
        }
        _V[i - 1][f] = nextVif;
    }

}
