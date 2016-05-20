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
import hivemall.utils.codec.ZigZagLEB128Codec;
import hivemall.utils.collections.IntOpenHashTable;
import hivemall.utils.io.IOUtils;
import hivemall.utils.lang.ObjectUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.roaringbitmap.RoaringBitmap;

public final class FFMPredictionModel implements Externalizable {

    private IntOpenHashTable<Entry> _map;
    private double _w0;
    private int _factors;
    private int _numFeatures;
    private int _numFields;

    public FFMPredictionModel() {}// for Externalizable

    public FFMPredictionModel(@Nonnull IntOpenHashTable<Entry> map, double w0, int factor,
            int numFeatures, int numFields) {
        this._map = map;
        this._w0 = w0;
        this._factors = factor;
        this._numFeatures = numFeatures;
        this._numFields = numFields;
    }

    public int getNumFactors() {
        return _factors;
    }

    public double getW0() {
        return _w0;
    }

    public int getNumFeatures() {
        return _numFeatures;
    }

    public int getNumFields() {
        return _numFields;
    }

    public int getActualNumFeatures() {
        return _map.size();
    }

    public long approxBytesConsumed() {
        int size = _map.size();
        // map
        long bytes = size * (1L + 4L + (4L * _factors));
        int rest = _map.capacity() - size;
        if (rest > 0) {
            bytes += rest * 4L;
        }
        // w0, factors, numFeatures, numFields, used
        bytes += (8 + 4 + 4 + 4 + 4);
        return bytes;
    }

    public float getW1(@Nonnull final Feature x) {
        int j = x.getFeatureIndex();

        Entry entry = _map.get(j);
        if (entry == null) {
            return 0.f;
        }
        return entry.W;
    }

    @Nullable
    public float[] getV(@Nonnull final Feature x, @Nonnull final int yField) {
        int j = Feature.toIntFeature(x, yField, _numFields);

        Entry entry = _map.get(j);
        if (entry == null) {
            return null;
        }
        return entry.Vf;
    }

    @Override
    public void writeExternal(@Nonnull ObjectOutput out) throws IOException {
        out.writeDouble(_w0);
        out.writeInt(_factors);
        out.writeInt(_numFeatures);
        out.writeInt(_numFields);

        final int factors = _factors;
        int used = _map.size();
        out.writeInt(used);

        final int[] keys = _map.getKeys();
        final int size = keys.length;
        out.writeInt(size);

        final Object[] values = _map.getValues();
        final RoaringBitmap emptyStatus = writeEmptyStates(_map.getStates(), out);

        for (int i = 0; i < size; i++) {
            if (emptyStatus.contains(i)) {
                continue;
            }
            ZigZagLEB128Codec.writeSignedVInt(keys[i], out);
            Entry v = (Entry) values[i];
            ZigZagLEB128Codec.writeFloat(v.W, out);
            IOUtils.writeVFloats(v.Vf, factors, out);
            values[i] = null; // help GC
        }
        this._map = null; // help GC        
    }

    @Nonnull
    private static RoaringBitmap writeEmptyStates(@Nonnull final byte[] status,
            @Nonnull final ObjectOutput out) throws IOException {
        final RoaringBitmap emptyBits = new RoaringBitmap();
        final int size = status.length;
        for (int i = 0; i < size; i++) {
            if (status[i] != IntOpenHashTable.FULL) {
                emptyBits.add(i);
            }
        }
        emptyBits.runOptimize();
        emptyBits.serialize(out);
        return emptyBits;
    }

    @Override
    public void readExternal(@Nonnull final ObjectInput in) throws IOException,
            ClassNotFoundException {
        this._w0 = in.readDouble();
        final int factors = in.readInt();
        this._factors = factors;
        this._numFeatures = in.readInt();
        this._numFields = in.readInt();

        int used = in.readInt();
        final int size = in.readInt();
        final int[] keys = new int[size];
        final Entry[] values = new Entry[size];
        final byte[] states = new byte[size];
        readStates(in, states);

        for (int i = 0; i < size; i++) {
            if (states[i] != IntOpenHashTable.FULL) {
                continue;
            }
            keys[i] = ZigZagLEB128Codec.readSignedVInt(in);
            float W = ZigZagLEB128Codec.readFloat(in);
            float[] Vf = IOUtils.readVFloats(in, factors);
            values[i] = new Entry(W, Vf);
        }

        this._map = new IntOpenHashTable<Entry>(keys, values, states, used);
    }

    @Nonnull
    private static void readStates(@Nonnull final ObjectInput in, @Nonnull final byte[] status)
            throws ClassNotFoundException, IOException {
        RoaringBitmap emptyBits = new RoaringBitmap();
        emptyBits.deserialize(in);

        Arrays.fill(status, IntOpenHashTable.FULL);
        for (int i : emptyBits) {
            status[i] = IntOpenHashTable.FREE;
        }
    }

    public byte[] serialize() throws IOException {
        return ObjectUtils.toCompressedBytes(this, true);
    }

    public static FFMPredictionModel deserialize(@Nonnull final byte[] serializedObj, final int len)
            throws ClassNotFoundException, IOException {
        FFMPredictionModel model = new FFMPredictionModel();
        ObjectUtils.readCompressedObject(serializedObj, len, model, true);
        return model;
    }

}
