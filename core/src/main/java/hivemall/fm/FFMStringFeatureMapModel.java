/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.fm;

import hivemall.fm.Entry.AdaGradEntry;
import hivemall.fm.Entry.FTRLEntry;
import hivemall.fm.FMHyperParameters.FFMHyperParameters;
import hivemall.utils.buffer.HeapBuffer;
import hivemall.utils.collections.Int2LongOpenHashTable;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.math.MathUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class FFMStringFeatureMapModel extends FieldAwareFactorizationMachineModel {
    private static final int DEFAULT_MAPSIZE = 65536;

    // LEARNING PARAMS
    private float _w0;
    @Nonnull
    private final Int2LongOpenHashTable _map;
    private final HeapBuffer _buf;

    // hyperparams
    private final int _numFeatures;
    private final int _numFields;

    // FTEL
    private final float _alpha;
    private final float _beta;
    private final float _lambda1;
    private final float _lamdda2;

    private final int _entrySize;

    public FFMStringFeatureMapModel(@Nonnull FFMHyperParameters params) {
        super(params);
        this._w0 = 0.f;
        this._map = new Int2LongOpenHashTable(DEFAULT_MAPSIZE);
        this._buf = new HeapBuffer(HeapBuffer.DEFAULT_CHUNK_SIZE);
        this._numFeatures = params.numFeatures;
        this._numFields = params.numFields;
        this._alpha = params.alphaFTRL;
        this._beta = params.betaFTRL;
        this._lambda1 = params.lambda1;
        this._lamdda2 = params.lamdda2;
        this._entrySize = entrySize(_factor, _useFTRL, _useAdaGrad);
    }

    @Nonnull
    FFMPredictionModel toPredictionModel() {
        return new FFMPredictionModel(_map, _buf, _w0, _factor, _numFeatures, _numFields);
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

        Entry entry = getEntry(j);
        if (entry == null) {
            return 0.f;
        }
        return entry.getW();
    }

    @Override
    protected void setW(@Nonnull final Feature x, final float nextWi) {
        final int j = x.getFeatureIndex();

        Entry entry = getEntry(j);
        if (entry == null) {
            float[] V = initV();
            entry = newEntry(nextWi, V);
            long ptr = entry.getOffset();
            _map.put(j, ptr);
        } else {
            entry.setW(nextWi);
        }
    }

    @Override
    void updateWi(final double dloss, @Nonnull final Feature x, final float eta) {
        final double Xi = x.getValue();
        float gradWi = (float) (dloss * Xi);

        final Entry theta = getEntry(x);
        float wi = theta.getW();

        float nextWi = wi - eta * (gradWi + 2.f * _lambdaW * wi);
        if (!NumberUtils.isFinite(nextWi)) {
            throw new IllegalStateException("Got " + nextWi + " for next W[" + x.getFeature()
                    + "]\n" + "Xi=" + Xi + ", gradWi=" + gradWi + ", wi=" + wi + ", dloss=" + dloss
                    + ", eta=" + eta);
        }
        theta.setW(nextWi);
    }

    /**
     * Update Wi using Follow-the-Regularized-Leader
     */
    boolean updateWiFTRL(final double dloss, @Nonnull final Feature x, final float eta) {
        final double Xi = x.getValue();
        float gradWi = (float) (dloss * Xi);

        final Entry theta = getEntry(x);
        float wi = theta.getW();

        final float z = theta.updateZ(gradWi, _alpha);
        final double n = theta.updateN(gradWi);

        if (Math.abs(z) <= _lambda1) {
            removeEntry(x);
            return wi != 0;
        }

        final float nextWi = (float) ((MathUtils.sign(z) * _lambda1 - z) / ((_beta + Math.sqrt(n))
                / _alpha + _lamdda2));
        if (!NumberUtils.isFinite(nextWi)) {
            throw new IllegalStateException("Got " + nextWi + " for next W[" + x.getFeature()
                    + "]\n" + "Xi=" + Xi + ", gradWi=" + gradWi + ", wi=" + wi + ", dloss=" + dloss
                    + ", eta=" + eta + ", n=" + n + ", z=" + z);
        }
        theta.setW(nextWi);
        return (nextWi != 0) || (wi != 0);
    }


    /**
     * @return V_x,yField,f
     */
    @Override
    public float getV(@Nonnull final Feature x, @Nonnull final int yField, final int f) {
        final int j = Feature.toIntFeature(x, yField, _numFields);

        Entry entry = getEntry(j);
        if (entry == null) {
            float[] V = initV();
            entry = newEntry(V);
            long ptr = entry.getOffset();
            _map.put(j, ptr);
        }
        return entry.getV(f);
    }

    @Override
    protected void setV(@Nonnull final Feature x, @Nonnull final int yField, final int f,
            final float nextVif) {
        final int j = Feature.toIntFeature(x, yField, _numFields);

        Entry entry = getEntry(j);
        if (entry == null) {
            float[] V = initV();
            entry = newEntry(V);
            long ptr = entry.getOffset();
            _map.put(j, ptr);
        }
        entry.setV(f, nextVif);
    }

    @Override
    protected Entry getEntry(@Nonnull final Feature x) {
        final int j = x.getFeatureIndex();

        Entry entry = getEntry(j);
        if (entry == null) {
            float[] V = initV();
            entry = newEntry(V);
            long ptr = entry.getOffset();
            _map.put(j, ptr);
        }
        return entry;
    }

    @Override
    protected Entry getEntry(@Nonnull final Feature x, @Nonnull final int yField) {
        final int j = Feature.toIntFeature(x, yField, _numFields);

        Entry entry = getEntry(j);
        if (entry == null) {
            float[] V = initV();
            entry = newEntry(V);
            long ptr = entry.getOffset();
            _map.put(j, ptr);
        }
        return entry;
    }

    protected void removeEntry(@Nonnull final Feature x) {
        int j = x.getFeatureIndex();
        _map.remove(j);
    }

    @Nonnull
    protected final Entry newEntry(final float W, @Nonnull final float[] V) {
        Entry entry = newEntry();
        entry.setW(W);
        entry.setV(V);
        return entry;
    }

    @Nonnull
    protected final Entry newEntry(@Nonnull final float[] V) {
        Entry entry = newEntry();
        entry.setV(V);
        return entry;
    }

    @Nonnull
    private Entry newEntry() {
        if (_useFTRL) {
            long ptr = _buf.allocate(_entrySize);
            return new FTRLEntry(_buf, _factor, ptr);
        } else if (_useAdaGrad) {
            long ptr = _buf.allocate(_entrySize);
            return new AdaGradEntry(_buf, _factor, ptr);
        } else {
            long ptr = _buf.allocate(_entrySize);
            return new Entry(_buf, _factor, ptr);
        }
    }

    @Nullable
    private Entry getEntry(final int key) {
        final long ptr = _map.get(key);
        if (ptr == -1L) {
            return null;
        }
        return getEntry(ptr);
    }

    @Nonnull
    private Entry getEntry(long ptr) {
        if (_useFTRL) {
            return new FTRLEntry(_buf, _factor, ptr);
        } else if (_useAdaGrad) {
            return new AdaGradEntry(_buf, _factor, ptr);
        } else {
            return new Entry(_buf, _factor, ptr);
        }
    }

    private static int entrySize(int factors, boolean ftrl, boolean adagrad) {
        if (ftrl) {
            return FTRLEntry.sizeOf(factors);
        } else if (adagrad) {
            return AdaGradEntry.sizeOf(factors);
        } else {
            return Entry.sizeOf(factors);
        }
    }

}
