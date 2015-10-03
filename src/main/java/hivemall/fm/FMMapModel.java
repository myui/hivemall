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
import hivemall.fm.FactorizationMachineUDTF.Feature;
import hivemall.utils.collections.IntOpenHashMap;
import hivemall.utils.math.MathUtils;

import java.util.Arrays;
import java.util.Random;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.metadata.HiveException;

public final class FMMapModel extends FactorizationMachineModel {
    private static final int DEFAULT_MAPSIZE = 1000;
    private static final Float FLOAT_ZERO = Float.valueOf(0.f);

    // LEARNING PARAMS
    private IntOpenHashMap<Float> _w;
    private IntOpenHashMap<float[]> _V;

    public FMMapModel(boolean classification, int factor, float lambda0, double sigma, long seed, double minTarget, double maxTarget, @Nonnull EtaEstimator eta) {
        super(classification, factor, lambda0, sigma, seed, minTarget, maxTarget, eta);
    }

    protected void initLearningParams() {
        this._w = new IntOpenHashMap<Float>(DEFAULT_MAPSIZE);
        this._V = new IntOpenHashMap<float[]>(DEFAULT_MAPSIZE);
    }

    @Override
    public int getSize() {
        return _w.size();
    }

    @Override
    public float getW(int i) {
        if(i == 0) {
            return _w0;
        } else {
            return _w.get(i);
        }
    }

    @Override
    public float getV(int i, int f) {
        final float[] Vi = _V.get(i);
        if(Vi == null) {
            return 0.f;
        }
        return Vi[f];
    }

    @Override
    protected void setW(int i, float nextWi) {
        assert (i >= 1) : i;
        _w.put(i, nextWi);
    }

    @Override
    public void setV(int i, int f, float nextVif) {
        assert (i >= 1) : i;
        float[] vi = _V.get(i);
        vi[f] = nextVif;
    }

    @Override
    public void check(@Nonnull final Feature[] x) throws HiveException {
        for(Feature e : x) {
            if(e == null) {
                continue;
            }
            final int idx = e.index;
            if(idx < 1) {
                throw new HiveException("Index of x should be greater than or equals to 1: "
                        + Arrays.toString(x));
            }
            if(!_w.containsKey(idx)) {
                _w.put(idx, FLOAT_ZERO);
            }
            if(!_V.containsKey(idx)) {
                float[] tmp = getRandomFloatArray(_factor, _sigma, _rnd);
                _V.put(idx, tmp);
            }
        }
    }

    private static float[] getRandomFloatArray(final int factor, final double sigma, @Nonnull final Random rnd) {
        final float[] ret = new float[factor];
        for(int i = 0, k = factor; i < k; i++) {
            ret[i] = (float) MathUtils.gaussian(0.d, sigma, rnd);
        }
        return ret;
    }

}
