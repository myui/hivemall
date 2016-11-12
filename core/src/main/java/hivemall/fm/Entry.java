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

import hivemall.utils.buffer.HeapBuffer;
import hivemall.utils.lang.NumberUtils;
import hivemall.utils.lang.SizeOf;

import javax.annotation.Nonnull;

class Entry {

    @Nonnull
    protected final HeapBuffer _buf;
    protected final int _size;
    protected final int _factors;

    protected long _offset;

    Entry(@Nonnull HeapBuffer buf, int factors) {
        this._buf = buf;
        this._size = Entry.sizeOf(factors);
        this._factors = factors;
    }

    Entry(@Nonnull HeapBuffer buf, int factors, long offset) {
        this(buf, factors, Entry.sizeOf(factors), offset);
    }

    private Entry(@Nonnull HeapBuffer buf, int factors, int size, long offset) {
        this._buf = buf;
        this._size = size;
        this._factors = factors;
        setOffset(offset);
    }

    int getSize() {
        return _size;
    }

    long getOffset() {
        return _offset;
    }

    void setOffset(long offset) {
        this._offset = offset;
    }

    float getW() {
        return _buf.getFloat(_offset);
    }

    void setW(final float value) {
        _buf.putFloat(_offset, value);
    }

    void getV(@Nonnull final float[] Vf) {
        final long offset = _offset + SizeOf.FLOAT;
        final int len = Vf.length;
        for (int i = 0; i < len; i++) {
            Vf[i] = _buf.getFloat(offset + SizeOf.FLOAT * i);
        }
    }

    void setV(@Nonnull final float[] Vf) {
        final long offset = _offset + SizeOf.FLOAT;
        final int len = Vf.length;
        for (int i = 0; i < len; i++) {
            _buf.putFloat(offset + SizeOf.FLOAT * i, Vf[i]);
        }
    }

    float getV(final int f) {
        return _buf.getFloat(_offset + SizeOf.FLOAT + SizeOf.FLOAT * f);
    }

    void setV(final int f, final float value) {
        long index = _offset + SizeOf.FLOAT + SizeOf.FLOAT * f;
        _buf.putFloat(index, value);
    }

    double getSumOfSquaredGradientsV() {
        throw new UnsupportedOperationException();
    }

    void addGradientV(float grad) {
        throw new UnsupportedOperationException();
    }

    float updateZ(float gradW, float alpha) {
        throw new UnsupportedOperationException();
    }

    double updateN(float gradW) {
        throw new UnsupportedOperationException();
    }

    static int sizeOf(int factors) {
        return SizeOf.FLOAT + SizeOf.FLOAT * factors;
    }

    static class AdaGradEntry extends Entry {

        final long _gg_offset;

        AdaGradEntry(@Nonnull HeapBuffer buf, int factors, long offset) {
            super(buf, factors, AdaGradEntry.sizeOf(factors), offset);
            this._gg_offset = _offset + SizeOf.FLOAT + SizeOf.FLOAT * _factors;
        }

        private AdaGradEntry(@Nonnull HeapBuffer buf, int factors, int size, long offset) {
            super(buf, factors, size, offset);
            this._gg_offset = _offset + SizeOf.FLOAT + SizeOf.FLOAT * _factors;
        }

        @Override
        double getSumOfSquaredGradientsV() {
            return _buf.getDouble(_gg_offset);
        }

        @Override
        void addGradientV(float grad) {
            double v = _buf.getDouble(_gg_offset);
            v += grad * grad;
            _buf.putDouble(_gg_offset, v);
        }

        static int sizeOf(int factors) {
            return Entry.sizeOf(factors) + SizeOf.DOUBLE;
        }

    }

    static final class FTRLEntry extends AdaGradEntry {

        final long _z_offset;

        FTRLEntry(@Nonnull HeapBuffer buf, int factors, long offset) {
            super(buf, factors, FTRLEntry.sizeOf(factors), offset);
            this._z_offset = _gg_offset + SizeOf.DOUBLE;
        }

        @Override
        float updateZ(float gradW, float alpha) {
            final float W = getW();
            final float z = getZ();
            final double n = getN();

            double gg = gradW * gradW;
            float sigma = (float) ((Math.sqrt(n + gg) - Math.sqrt(n)) / alpha);

            final float newZ = z + gradW - sigma * W;
            if (!NumberUtils.isFinite(newZ)) {
                throw new IllegalStateException("Got newZ " + newZ + " where z=" + z + ", gradW="
                        + gradW + ", sigma=" + sigma + ", W=" + W + ", n=" + n + ", gg=" + gg
                        + ", alpha=" + alpha);
            }
            setZ(newZ);
            return newZ;
        }

        private float getZ() {
            return _buf.getFloat(_z_offset);
        }

        private void setZ(final float value) {
            _buf.putFloat(_z_offset, value);
        }

        @Override
        double updateN(final float gradW) {
            final double n = getN();

            final double newN = n + gradW * gradW;
            if (!NumberUtils.isFinite(newN)) {
                throw new IllegalStateException("Got newN " + newN + " where n=" + n + ", gradW="
                        + gradW);
            }
            setN(newN);
            return newN;
        }

        private double getN() {
            long index = _z_offset + SizeOf.FLOAT;
            return _buf.getDouble(index);
        }

        private void setN(final double value) {
            long index = _z_offset + SizeOf.FLOAT;
            _buf.putDouble(index, value);
        }

        static int sizeOf(int factors) {
            return AdaGradEntry.sizeOf(factors) + SizeOf.FLOAT + SizeOf.DOUBLE;
        }

    }
}
