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

import hivemall.utils.collections.IMapIterator;
import hivemall.utils.collections.OpenHashTable;

import javax.annotation.Nonnull;

public final class FMStringFeatureMapModel extends FactorizationMachineModel {
    private static final int DEFAULT_MAPSIZE = 4096;

    // LEARNING PARAMS
    private float _w0;
    private final OpenHashTable<String, Entry> _map;

    public FMStringFeatureMapModel(@Nonnull FMHyperParameters params) {
        super(params);
        this._w0 = 0.f;
        this._map = new OpenHashTable<String, FMStringFeatureMapModel.Entry>(DEFAULT_MAPSIZE);
    }

    @Override
    public int getSize() {
        return _map.size();
    }

    IMapIterator<String, Entry> entries() {
        return _map.entries();
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
    public float getW(@Nonnull Feature x) {
        String j = x.getFeature();
        assert (j != null);

        Entry entry = _map.get(j);
        if (entry == null) {
            return 0.f;
        }
        return entry.W;
    }

    @Override
    protected void setW(@Nonnull Feature x, float nextWi) {
        String j = x.getFeature();
        assert (j != null);

        Entry entry = _map.get(j);
        if (entry == null) {
            float[] Vf = initV();
            entry = new Entry(nextWi, Vf);
            _map.put(j, entry);
        } else {
            entry.W = nextWi;
        }
    }

    @Override
    public float getV(@Nonnull Feature x, int f) {
        String j = x.getFeature();
        assert (j != null);

        final float[] V;
        Entry entry = _map.get(j);
        if (entry == null) {
            V = initV();
            entry = new Entry(0.f, V);
            _map.put(j, entry);
        } else {
            V = entry.Vf;
            assert (V != null);
        }
        return V[f];
    }

    @Override
    protected void setV(@Nonnull Feature x, int f, float nextVif) {
        String j = x.getFeature();
        assert (j != null);

        final float[] V;
        Entry entry = _map.get(j);
        if (entry == null) {
            V = initV();
            entry = new Entry(0.f, V);
            _map.put(j, entry);
        } else {
            V = entry.Vf;
            assert (V != null);
        }
        V[f] = nextVif;
    }

    static final class Entry {

        float W;
        @Nonnull
        final float[] Vf;

        Entry(float W, @Nonnull float[] Vf) {
            this.W = W;
            this.Vf = Vf;
        }

    }

}
