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
package hivemall.utils.buffer;

import hivemall.utils.lang.Preconditions;
import hivemall.utils.lang.SizeOf;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

public class HeapBufferTest {

    @Test
    public void testSingleEntry() {
        final int factors = 5;
        HeapBuffer heap = new HeapBuffer(HeapBuffer.DEFAULT_CHUNK_SIZE);

        Entry1 e1 = new Entry1(heap, factors);
        long ptr1 = heap.allocate(e1.size());
        e1.setOffset(ptr1);

        float w1 = 1.f;
        float[] v1 = new float[factors];
        for (int i = 0; i < factors; i++) {
            v1[i] = i;
        }

        e1.setW(w1);
        e1.setV(v1);

        float[] vProbe = new float[factors];
        Assert.assertEquals(w1, e1.getW(), 0.f);
        e1.getV(vProbe);
        Assert.assertArrayEquals(v1, vProbe, 0.f);
    }

    @Test
    public void testMultipleEntries() {
        final int factors = 5;
        final HeapBuffer buf = new HeapBuffer(HeapBuffer.DEFAULT_CHUNK_SIZE);

        Entry1 e1 = new Entry1(buf, factors);
        long ptr1 = buf.allocate(e1.size());
        e1.setOffset(ptr1);

        float w1 = 1.f;
        float[] v1 = new float[factors];
        for (int i = 0; i < factors; i++) {
            v1[i] = 1 + i;
        }

        e1.setW(w1);
        e1.setV(v1);

        float[] vProbe = new float[factors];
        Assert.assertEquals(w1, e1.getW(), 0.f);
        e1.getV(vProbe);
        Assert.assertArrayEquals(v1, vProbe, 0.f);

        w1 = 1.1f;
        e1.setW(w1);
        Assert.assertEquals(w1, e1.getW(), 0.f);

        Assert.assertEquals(e1.size(), buf.getAllocatedBytes());

        Entry2 e2 = new Entry2(buf, factors);
        long ptr2 = buf.allocate(e2.size());
        e2.setOffset(ptr2);

        float w2 = 2.f;
        float[] v2 = new float[factors];
        for (int i = 0; i < factors; i++) {
            v2[i] = 2 + i;
        }
        double z2 = 2.d;

        e2.setW(w2);
        e2.setV(v2);
        e2.setZ(z2);

        Assert.assertEquals(w2, e2.getW(), 0.f);
        e2.getV(vProbe);
        Assert.assertArrayEquals(v2, vProbe, 0.f);
        Assert.assertEquals(z2, e2.getZ(), 0.d);
        for (int i = 0; i < factors; i++) {
            v2[i] = 2.1f + i;
        }
        e2.setV(v2);
        Assert.assertEquals(z2, e2.getZ(), 0.d);

        Assert.assertEquals(w1, e1.getW(), 0.f);
        e1.getV(vProbe);
        Assert.assertArrayEquals(v1, vProbe, 0.f);

        Assert.assertEquals(e1.size() + e2.size(), buf.getAllocatedBytes());
    }

    @Test
    public void testLargeEntries() {
        final int loop = 10000;
        final int factors = 5;
        final float[] v = new float[factors];
        final HeapBuffer buf = new HeapBuffer(1024, 2);
        final long[] ptrs = new long[loop];

        final Entry1 entryProbe = new Entry1(buf, factors);
        for (int i = 0; i < loop; i++) {
            entryProbe.setOffset(buf.allocate(entryProbe.size()));
            ptrs[i] = entryProbe.getOffset();
            entryProbe.setW(i);
            for (int f = 0; f < factors; f++) {
                v[f] = i + f;
            }
            entryProbe.setV(v);
        }

        for (int i = 0; i < loop; i++) {
            entryProbe.setOffset(ptrs[i]);
            Assert.assertEquals(i, entryProbe.getW(), 0.f);
            entryProbe.getV(v);
            for (int f = 0; f < factors; f++) {
                Assert.assertEquals(i + f, v[f], 0.f);
            }
        }
    }

    private static class Entry1 {

        @Nonnull
        final HeapBuffer buf;
        final int size;
        final int factors;
        long offset;

        Entry1(@Nonnull HeapBuffer buf, int size, int factors) {
            this.buf = buf;
            this.size = size;
            this.factors = factors;
        }

        Entry1(@Nonnull HeapBuffer buf, int factors) {
            this(buf, getSize(factors), factors);
        }

        int size() {
            return size;
        }

        void setOffset(long offset) {
            this.offset = offset;
        }

        long getOffset() {
            return offset;
        }

        void setW(float w) {
            buf.putFloat(offset, w);
        }

        float getW() {
            return buf.getFloat(offset);
        }

        void setV(@Nonnull float[] v) {
            Preconditions.checkArgument(v.length == factors);
            buf.putFloats(offset + SizeOf.FLOAT, v);
        }

        void getV(@Nonnull float[] v) {
            Preconditions.checkArgument(v.length == factors);
            buf.getFloats(offset + SizeOf.FLOAT, v);
        }

        static int getSize(int factors) {
            return SizeOf.FLOAT + SizeOf.FLOAT * factors;
        }
    }

    private static class Entry2 extends Entry1 {

        Entry2(@Nonnull HeapBuffer buf, int factors) {
            super(buf, getSize(factors), factors);
        }

        void setZ(double z) {
            buf.putDouble(offset + SizeOf.FLOAT + SizeOf.FLOAT * factors, z);
        }

        double getZ() {
            return buf.getDouble(offset + SizeOf.FLOAT + SizeOf.FLOAT * factors);
        }

        static int getSize(int factors) {
            return Entry1.getSize(factors) + SizeOf.DOUBLE;
        }
    }

}
