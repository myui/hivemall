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

import hivemall.fm.FMHyperParameters.FFMHyperParameters;
import hivemall.utils.collections.IntOpenHashTable;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.math.MathUtils;

import javax.annotation.Nonnull;

public final class FFMStringFeatureMapModel extends FieldAwareFactorizationMachineModel {
    private static final int DEFAULT_MAPSIZE = 65536;

    // LEARNING PARAMS
    private float _w0;
    private final IntOpenHashTable<Entry> _map;

    // hyperparams
    private final int _numFeatures;
    private final int _numFields;

    // FTEL
    private final float _alpha;
    private final float _beta;
    private final float _lambda1;
    private final float _lamdda2;

    public FFMStringFeatureMapModel(@Nonnull FFMHyperParameters params) {
        super(params);
        this._w0 = 0.f;
        this._map = new IntOpenHashTable<FFMStringFeatureMapModel.Entry>(DEFAULT_MAPSIZE);
        this._numFeatures = params.numFeatures;
        this._numFields = params.numFields;
        this._alpha = params.alphaFTRL;
        this._beta = params.betaFTRL;
        this._lambda1 = params.lambda1;
        this._lamdda2 = params.lamdda2;
    }

    @Nonnull
    FFMPredictionModel toPredictionModel() {
        return new FFMPredictionModel(_map, _w0, _factor, _numFeatures, _numFields);
    }

    @Override
    public int getSize() {
        return _map.size();
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
    public float getW(@Nonnull final Feature x) {
        int j = x.getFeatureIndex();

        Entry entry = _map.get(j);
        if (entry == null) {
            return 0.f;
        }
        return entry.W;
    }

    @Override
    protected void setW(@Nonnull final Feature x, final float nextWi) {
        final int j = x.getFeatureIndex();

        Entry entry = _map.get(j);
        if (entry == null) {
            entry = newEntry(nextWi);
            _map.put(j, entry);
        } else {
            entry.W = nextWi;
        }
    }

    @Override
    void updateWi(final double dloss, @Nonnull final Feature x, final float eta) {
        final double Xi = x.getValue();
        float gradWi = (float) (dloss * Xi);

        final Entry theta = getEntry(x);
        float wi = theta.W;

        float nextWi = wi - eta * (gradWi + 2.f * _lambdaW * wi);
        if (!NumberUtils.isFinite(nextWi)) {
            throw new IllegalStateException("Got " + nextWi + " for next W[" + x.getFeature()
                    + "]\n" + "Xi=" + Xi + ", gradWi=" + gradWi + ", wi=" + wi + ", dloss=" + dloss
                    + ", eta=" + eta);
        }
        theta.W = nextWi;
    }

    /**
     * Update Wi using Follow-the-Regularized-Leader
     */
    boolean updateWiFTRL(final double dloss, @Nonnull final Feature x, final float eta) {
        final double Xi = x.getValue();
        float gradWi = (float) (dloss * Xi);

        final Entry theta = getEntry(x);
        float wi = theta.W;

        final float z = theta.updateZ(gradWi, _alpha);
        final double n = theta.updateN(gradWi);

        final float nextWi;
        if (Math.abs(z) <= _lambda1) {
            nextWi = 0.f;
        } else {
            nextWi = (float) (-(z - MathUtils.sign(z) * _lambda1) / ((_beta + Math.sqrt(n))
                    / _alpha + _lamdda2));
            if (!NumberUtils.isFinite(nextWi)) {
                throw new IllegalStateException("Got " + nextWi + " for next W[" + x.getFeature()
                        + "]\n" + "Xi=" + Xi + ", gradWi=" + gradWi + ", wi=" + wi + ", dloss="
                        + dloss + ", eta=" + eta + ", n=" + n + ", z=" + z);
            }
        }

        theta.W = nextWi;
        return (nextWi != 0) || (wi != 0);
    }


    /**
     * @return V_x,yField,f
     */
    @Override
    public float getV(@Nonnull final Feature x, @Nonnull final int yField, final int f) {
        final int j = Feature.toIntFeature(x, yField, _numFields);

        final float[] V;
        Entry entry = _map.get(j);
        if (entry == null) {
            V = initV();
            entry = newEntry(V);
            _map.put(j, entry);
        } else {
            if (entry.Vf == null) {
                V = initV();
                entry.Vf = V;
            } else {
                V = entry.Vf;
            }
        }
        return V[f];
    }

    @Override
    protected void setV(@Nonnull final Feature x, @Nonnull final int yField, final int f,
            final float nextVif) {
        final int j = Feature.toIntFeature(x, yField, _numFields);

        final float[] V;
        Entry entry = _map.get(j);
        if (entry == null) {
            V = initV();
            entry = newEntry(V);
            _map.put(j, entry);
        } else {
            if (entry.Vf == null) {
                V = initV();
                entry.Vf = V;
            } else {
                V = entry.Vf;
            }
        }
        V[f] = nextVif;
    }

    @Override
    protected Entry getEntry(@Nonnull final Feature x) {
        final int j = x.getFeatureIndex();
        Entry entry = _map.get(j);
        if (entry == null) {
            entry = newEntry(0.f);
            _map.put(j, entry);
        }
        return entry;
    }

    @Override
    protected Entry getEntry(@Nonnull final Feature x, @Nonnull final int yField) {
        final int j = Feature.toIntFeature(x, yField, _numFields);
        Entry entry = _map.get(j);
        if (entry == null) {
            float[] V = initV();
            entry = newEntry(V);
            _map.put(j, entry);
        } else {
            if (entry.Vf == null) {
                entry.Vf = initV();
            }
        }
        return entry;
    }

}
