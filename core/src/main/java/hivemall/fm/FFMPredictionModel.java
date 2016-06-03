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

import hivemall.utils.buffer.HeapBuffer;
import hivemall.utils.collections.Int2LongOpenHashTable;
import hivemall.utils.io.CompressionStreamFactory.CompressionAlgorithm;
import hivemall.utils.lang.ObjectUtils;
import hivemall.utils.lang.SizeOf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class FFMPredictionModel implements Externalizable {

    /**
     * maps feature to feature weight pointer
     */
    private Int2LongOpenHashTable _map;
    private HeapBuffer _buf;

    private double _w0;
    private int _factors;
    private int _numFeatures;
    private int _numFields;

    public FFMPredictionModel() {}// for Externalizable

    public FFMPredictionModel(@Nonnull Int2LongOpenHashTable map, @Nonnull HeapBuffer buf,
            double w0, int factor, int numFeatures, int numFields) {
        this._map = map;
        this._buf = buf;
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
        // map: size * (|state| + |key| + |value|) 
        int size = _map.size();
        long bytes = size * (1L + SizeOf.INT + SizeOf.LONG);
        // buf
        bytes += _buf.consumedBytes();
        // w0, factors, numFeatures, numFields
        bytes += (8 + 4 + 4 + 4);
        return bytes;
    }

    @Nullable
    private Entry getEntry(final int key) {
        final long ptr = _map.get(key);
        if (ptr == -1L) {
            return null;
        }
        return new Entry(_buf, _factors, ptr);
    }

    public float getW(@Nonnull final Feature x) {
        int j = x.getFeatureIndex();

        Entry entry = getEntry(j);
        if (entry == null) {
            return 0.f;
        }
        return entry.getW();
    }

    /**
     * @return true if V exists
     */
    public boolean getV(@Nonnull final Feature x, @Nonnull final int yField, @Nonnull float[] dst) {
        int j = Feature.toIntFeature(x, yField, _numFields);

        Entry entry = getEntry(j);
        if (entry == null) {
            return false;
        }
        entry.getV(dst);
        return true;
    }

    @Override
    public void writeExternal(@Nonnull ObjectOutput out) throws IOException {
        out.writeDouble(_w0);
        out.writeInt(_factors);
        out.writeInt(_numFeatures);
        out.writeInt(_numFields);

        _map.writeExternal(out);
        this._map = null;
        _buf.writeExternal(out);
        this._buf = null;
    }

    @Override
    public void readExternal(@Nonnull final ObjectInput in) throws IOException,
            ClassNotFoundException {
        this._w0 = in.readDouble();
        this._factors = in.readInt();
        this._numFeatures = in.readInt();
        this._numFields = in.readInt();

        this._map = new Int2LongOpenHashTable();
        _map.readExternal(in);
        this._buf = new HeapBuffer();
        _buf.readExternal(in);
    }

    public byte[] serialize() throws IOException {
        return ObjectUtils.toCompressedBytes(this, CompressionAlgorithm.lzma2, true);
    }

    public static FFMPredictionModel deserialize(@Nonnull final byte[] serializedObj, final int len)
            throws ClassNotFoundException, IOException {
        FFMPredictionModel model = new FFMPredictionModel();
        ObjectUtils.readCompressedObject(serializedObj, len, model, CompressionAlgorithm.lzma2,
            true);
        return model;
    }

}
