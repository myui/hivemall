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
import hivemall.utils.codec.VariableByteCodec;
import hivemall.utils.codec.ZigZagLEB128Codec;
import hivemall.utils.collections.Int2LongOpenHashTable;
import hivemall.utils.collections.IntOpenHashTable;
import hivemall.utils.io.CompressionStreamFactory.CompressionAlgorithm;
import hivemall.utils.io.IOUtils;
import hivemall.utils.lang.ArrayUtils;
import hivemall.utils.lang.HalfFloat;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class FFMPredictionModel implements Externalizable {
    private static final Log LOG = LogFactory.getLog(FFMPredictionModel.class);

    private static final byte HALF_FLOAT_ENTRY = 1;
    private static final byte W_ONLY_HALF_FLOAT_ENTRY = 2;
    private static final byte FLOAT_ENTRY = 3;
    private static final byte W_ONLY_FLOAT_ENTRY = 4;

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
        int size = _map.size();

        // [map] size * (|state| + |key| + |entry|) 
        long bytes = size * (1L + 4L + 4L + (4L * _factors));
        int rest = _map.capacity() - size;
        if (rest > 0) {
            bytes += rest * 1L;
        }
        // w0, factors, numFeatures, numFields, used, size
        bytes += (8 + 4 + 4 + 4 + 4 + 4);
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
        if (ArrayUtils.equals(dst, 0.f)) {
            return false; // treat as null
        }
        return true;
    }

    @Override
    public void writeExternal(@Nonnull ObjectOutput out) throws IOException {
        out.writeDouble(_w0);
        final int factors = _factors;
        out.writeInt(factors);
        out.writeInt(_numFeatures);
        out.writeInt(_numFields);

        int used = _map.size();
        out.writeInt(used);

        final int[] keys = _map.getKeys();
        final int size = keys.length;
        out.writeInt(size);

        final byte[] states = _map.getStates();
        writeStates(states, out);

        final long[] values = _map.getValues();

        final HeapBuffer buf = _buf;
        final Entry e = new Entry(buf, factors);
        final float[] Vf = new float[factors];
        for (int i = 0; i < size; i++) {
            if (states[i] != IntOpenHashTable.FULL) {
                continue;
            }
            ZigZagLEB128Codec.writeSignedInt(keys[i], out);
            e.setOffset(values[i]);
            writeEntry(e, factors, Vf, out);
        }

        // help GC
        this._map = null;
        this._buf = null;
    }

    private static void writeEntry(@Nonnull final Entry e, final int factors,
            @Nonnull final float[] Vf, @Nonnull final DataOutput out) throws IOException {
        final float W = e.getW();
        e.getV(Vf);

        if (ArrayUtils.almostEquals(Vf, 0.f)) {
            if (HalfFloat.isRepresentable(W)) {
                out.writeByte(W_ONLY_HALF_FLOAT_ENTRY);
                out.writeShort(HalfFloat.floatToHalfFloat(W));
            } else {
                out.writeByte(W_ONLY_FLOAT_ENTRY);
                out.writeFloat(W);
            }
        } else if (isRepresentableAsHalfFloat(W, Vf)) {
            out.writeByte(HALF_FLOAT_ENTRY);
            out.writeShort(HalfFloat.floatToHalfFloat(W));
            for (int i = 0; i < factors; i++) {
                out.writeShort(HalfFloat.floatToHalfFloat(Vf[i]));
            }
        } else {
            out.writeByte(FLOAT_ENTRY);
            out.writeFloat(W);
            IOUtils.writeFloats(Vf, factors, out);
        }
    }

    private static boolean isRepresentableAsHalfFloat(final float W, @Nonnull final float[] Vf) {
        if (!HalfFloat.isRepresentable(W)) {
            return false;
        }
        for (float V : Vf) {
            if (!HalfFloat.isRepresentable(V)) {
                return false;
            }
        }
        return true;
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

        final int used = in.readInt();
        final int size = in.readInt();
        final int[] keys = new int[size];
        final long[] values = new long[size];
        final byte[] states = new byte[size];
        readStates(in, states);

        final int entrySize = Entry.sizeOf(factors);
        int numChunks = (entrySize * used) / HeapBuffer.DEFAULT_CHUNK_BYTES + 1;
        final HeapBuffer buf = new HeapBuffer(HeapBuffer.DEFAULT_CHUNK_SIZE, numChunks);
        final Entry e = new Entry(buf, factors);
        final float[] Vf = new float[factors];
        for (int i = 0; i < size; i++) {
            if (states[i] != IntOpenHashTable.FULL) {
                continue;
            }
            keys[i] = ZigZagLEB128Codec.readSignedInt(in);
            long ptr = buf.allocate(entrySize);
            e.setOffset(ptr);
            readEntry(in, factors, Vf, e);
            values[i] = ptr;
        }

        this._map = new Int2LongOpenHashTable(keys, values, states, used);
        this._buf = buf;
    }

    @Nonnull
    private static void readEntry(@Nonnull final DataInput in, final int factors,
            @Nonnull final float[] Vf, @Nonnull Entry dst) throws IOException {
        final byte type = in.readByte();
        switch (type) {
            case HALF_FLOAT_ENTRY: {
                float W = HalfFloat.halfFloatToFloat(in.readShort());
                dst.setW(W);
                for (int i = 0; i < factors; i++) {
                    Vf[i] = HalfFloat.halfFloatToFloat(in.readShort());
                }
                dst.setV(Vf);
                break;
            }
            case W_ONLY_HALF_FLOAT_ENTRY: {
                float W = HalfFloat.halfFloatToFloat(in.readShort());
                dst.setW(W);
                break;
            }
            case FLOAT_ENTRY: {
                float W = in.readFloat();
                dst.setW(W);
                IOUtils.readFloats(in, Vf);
                dst.setV(Vf);
                break;
            }
            case W_ONLY_FLOAT_ENTRY: {
                float W = in.readFloat();
                dst.setW(W);
                break;
            }
            default:
                throw new IOException("Unexpected Entry type: " + type);
        }
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
        LOG.info("FFMPredictionModel#serialize(): " + _buf.toString());
        return ObjectUtils.toCompressedBytes(this, CompressionAlgorithm.lzma2, true);
    }

    public static FFMPredictionModel deserialize(@Nonnull final byte[] serializedObj, final int len)
            throws ClassNotFoundException, IOException {
        FFMPredictionModel model = new FFMPredictionModel();
        ObjectUtils.readCompressedObject(serializedObj, len, model, CompressionAlgorithm.lzma2,
            true);
        LOG.info("FFMPredictionModel#deserialize(): " + model._buf.toString());
        return model;
    }

}
