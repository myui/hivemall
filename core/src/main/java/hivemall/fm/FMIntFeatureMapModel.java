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
import hivemall.utils.collections.Int2FloatOpenHash;
import hivemall.utils.collections.IntOpenHashMap;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public final class FMIntFeatureMapModel extends FactorizationMachineModel {
    private static final int DEFAULT_MAPSIZE = 4096;

    // LEARNING PARAMS
    private float _w0;
    private final Int2FloatOpenHash _w;
    private final IntOpenHashMap<float[]> _V;

    private int _minIndex, _maxIndex;

    public FMIntFeatureMapModel(boolean classification, int factor, float lambda0, double sigma,
            long seed, double minTarget, double maxTarget, @Nonnull EtaEstimator eta,
            @Nonnull VInitScheme vInit) {
        super(classification, factor, lambda0, sigma, seed, minTarget, maxTarget, eta, vInit);
        this._w0 = 0.f;
        this._w = new Int2FloatOpenHash(DEFAULT_MAPSIZE);
        _w.defaultReturnValue(0.f);
        this._V = new IntOpenHashMap<float[]>(DEFAULT_MAPSIZE);
        this._minIndex = 0;
        this._maxIndex = 0;
    }

    @Override
    public int getSize() {
        return _w.size();
    }

    @Override
    protected int getMinIndex() {
        return _minIndex;
    }

    @Override
    protected int getMaxIndex() {
        return _maxIndex;
    }

    @Override
    public float getW0() {
        return _w0;
    }

    @Override
    protected void setW0(float nextW0) {
        this._w0 = nextW0;
    }

    @Override
    protected float getW(final int i) {
        assert (i >= 1) : i;
        return _w.get(i);
    }

    @Override
    public float getW(@Nonnull final Feature x) {
        final int i = x.getFeatureIndex();
        if (i == 0) {
            return _w0;
        } else {
            assert (i >= 1) : i;
            return _w.get(i);
        }
    }

    @Override
    protected void setW(@Nonnull Feature x, float nextWi) {
        final int i = x.getFeatureIndex();
        if (i == 0) {
            this._w0 = nextWi;
        } else {
            assert (i >= 1) : i;
            _w.put(i, nextWi);
        }
    }

    @Override
    protected float[] getV(int i) {
        assert (i >= 1) : i;
        return _V.get(i);
    }

    @Override
    public float getV(@Nonnull final Feature x, final int f) {
        int i = x.getFeatureIndex();
        assert (i >= 1) : i;
        final float[] Vi = _V.get(i);
        if (Vi == null) {
            return 0.f;
        }
        return Vi[f];
    }

    @Override
    protected void setV(@Nonnull Feature x, int f, float nextVif) {
        final int i = x.getFeatureIndex();
        assert (i >= 1) : i;
        float[] vi = _V.get(i);
        assert (vi != null) : "V[" + i + "] was null";
        vi[f] = nextVif;
    }

    @Override
    public void check(@Nonnull final Feature[] x) throws HiveException {
        for (Feature e : x) {
            if (e == null) {
                continue;
            }
            final int idx = e.getFeatureIndex();
            if (idx < 1) {
                throw new HiveException("Index of x should be greater than or equals to 1: "
                        + Arrays.toString(x));
            }
            if (!_w.containsKey(idx)) {
                _w.put(idx, 0.f);
            }
            if (!_V.containsKey(idx)) {
                float[] tmp = initV();
                _V.put(idx, tmp);
            }
            this._maxIndex = Math.max(_maxIndex, idx);
            this._minIndex = Math.min(_minIndex, idx);
        }
    }
}
