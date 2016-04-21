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

import hivemall.fm.FieldAwareFactorizationMachineModel.Entry;
import hivemall.utils.collections.OpenHashTable;

import javax.annotation.Nonnull;

public final class FFMPredictionModel {

    private int _factors;
    private double _w0;
    private OpenHashTable<String, Entry> _map;

    public FFMPredictionModel() {}
    
    public FFMPredictionModel(String ser) {
        //TODO
    }

    public int getNumFactors() {
        return _factors;
    }

    public double getW0() {
        return _w0;
    }

    public float getW1(@Nonnull Feature e) {
        return _map.get(e.getFeature()).W;
    }

    public float getV(@Nonnull Feature x, @Nonnull String field, int f) {
        String j = getFeature(x, field);

        final float[] V;
        Entry entry = _map.get(j);
        if (entry == null) {
            return 0.f;
        } else {
            V = entry.Vf;
            assert (V != null);
        }
        return V[f];
    }

    @Nonnull
    private static String getFeature(@Nonnull Feature x, @Nonnull String yField) {
        return x.getFeature() + ':' + yField;
    }
}
