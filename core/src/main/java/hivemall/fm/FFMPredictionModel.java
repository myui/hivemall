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
import hivemall.utils.codec.VariableByteCodec;
import hivemall.utils.codec.ZigZagLEB128Codec;
import hivemall.utils.collections.IntOpenHashTable;
import hivemall.utils.io.CompressionStreamFactory.CompressionAlgorithm;
import hivemall.utils.io.IOUtils;
import hivemall.utils.lang.ObjectUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
            bytes += rest * 1L;
        }
        // w0, factors, numFeatures, numFields, used, size
        bytes += (8 + 4 + 4 + 4 + 4 + 4);
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
        final byte[] states = _map.getStates();
        writeStates(states, out);

        for (int i = 0; i < size; i++) {
            if (states[i] != IntOpenHashTable.FULL) {
                continue;
            }
            ZigZagLEB128Codec.writeSignedInt(keys[i], out);
            Entry v = (Entry) values[i];
            out.writeFloat(v.W);
            IOUtils.writeFloats(v.Vf, factors, out);
            values[i] = null; // help GC
        }
        this._map = null; // help GC        
    }

    @Nonnull
    static void writeStates(@Nonnull final byte[] status, @Nonnull final DataOutput out)
            throws IOException {
        // write empty states's indexes differentially
        final int size = status.length;
        int cardinarity = 0;
        for (int i = 0; i < size; i++) {
            if (status[i] != IntOpenHashTable.FULL) {
                cardinarity++;
            }
        }
        out.writeInt(cardinarity);
        if (cardinarity == 0) {
            return;
        }
        int prev = 0;
        for (int i = 0; i < size; i++) {
            if (status[i] != IntOpenHashTable.FULL) {
                int diff = i - prev;
                assert (diff >= 0);
                VariableByteCodec.encodeUnsignedInt(diff, out);
                prev = i;
            }
        }
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
            keys[i] = ZigZagLEB128Codec.readSignedInt(in);
            float W = in.readFloat();
            float[] Vf = IOUtils.readFloats(in, factors);
            values[i] = new Entry(W, Vf);
        }

        this._map = new IntOpenHashTable<Entry>(keys, values, states, used);
    }

    @Nonnull
    static void readStates(@Nonnull final DataInput in, @Nonnull final byte[] status)
            throws IOException {
        // read non-empty states differentially
        final int cardinarity = in.readInt();
        Arrays.fill(status, IntOpenHashTable.FULL);
        int prev = 0;
        for (int j = 0; j < cardinarity; j++) {
            int i = VariableByteCodec.decodeUnsignedInt(in) + prev;
            status[i] = IntOpenHashTable.FREE;
            prev = i;
        }
    }

    public byte[] serialize() throws IOException {
        return ObjectUtils.toCompressedBytes(this, CompressionAlgorithm.deflate_l7, true);
    }

    public static FFMPredictionModel deserialize(@Nonnull final byte[] serializedObj, final int len)
            throws ClassNotFoundException, IOException {
        FFMPredictionModel model = new FFMPredictionModel();
        ObjectUtils.readCompressedObject(serializedObj, len, model, CompressionAlgorithm.deflate,
            true);
        return model;
    }

}
