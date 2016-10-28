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
package hivemall.utils.collections;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

public class DoubleRingBufferTest {

    @Test
    public void testAdd() {
        final int capacity = 3;
        final double[] dst = new double[capacity];
        DoubleRingBuffer ring = new DoubleRingBuffer(capacity);
        ring.add(1);
        Assert.assertEquals(capacity, ring.capacity());
        Assert.assertEquals(1, ring.size());
        ring.add(2);
        ring.add(3);
        ring.toArray(dst);
        Assert.assertArrayEquals(new double[] {1, 2, 3}, dst, 0.d);
        ring.add(4);
        Assert.assertEquals(capacity, ring.size());
        ring.toArray(dst);
        Assert.assertArrayEquals(new double[] {2, 3, 4}, dst, 0.d);
        ring.add(5);
        ring.add(6);
        ring.toArray(dst);
        Assert.assertArrayEquals(new double[] {4, 5, 6}, dst, 0.d);
        ring.add(7);
        ring.toArray(dst);
        Assert.assertArrayEquals(new double[] {5, 6, 7}, dst, 0.d);
    }

    @Test
    public void testIterator() {
        DoubleRingBuffer ring = new DoubleRingBuffer(3);
        ring.add(1);
        ring.add(2);
        ring.add(3);
        ring.add(4);
        Iterator<Double> itor = ring.iterator();
        Assert.assertTrue(itor.hasNext());
        Assert.assertEquals(2, itor.next().intValue());
        Assert.assertEquals(3, itor.next().intValue());
        Assert.assertEquals(4, itor.next().intValue());
        Assert.assertFalse(itor.hasNext());
    }

    @Test
    public void testFifoLifo() {
        final int capacity = 3;
        final double[] dst = new double[capacity];
        DoubleRingBuffer ring = new DoubleRingBuffer(capacity);
        ring.add(1);
        ring.add(2);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {1, 2, 0}, dst, 0.d);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {2, 1, 0}, dst, 0.d);
        ring.add(3);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {1, 2, 3}, dst, 0.d);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {3, 2, 1}, dst, 0.d);
        ring.add(4);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {2, 3, 4}, dst, 0.d);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {4, 3, 2}, dst, 0.d);
    }

    @Test
    public void testLifo() {
        final double[] dst = new double[] {-1, -1, -1};
        DoubleRingBuffer ring = new DoubleRingBuffer(3);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {0, 0, 0}, dst, 0.d);
        ring.add(1);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {1, 0, 0}, dst, 0.d);
        ring.add(2);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {2, 1, 0}, dst, 0.d);
        ring.add(3);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {3, 2, 1}, dst, 0.d);
        ring.add(4);
        ring.toArray(dst, false);
        Assert.assertArrayEquals(new double[] {4, 3, 2}, dst, 0.d);
    }

    @Test
    public void testFifo() {
        final double[] dst = new double[] {-1, -1, -1};
        DoubleRingBuffer ring = new DoubleRingBuffer(3);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {0, 0, 0}, dst, 0.d);
        ring.add(1);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {1, 0, 0}, dst, 0.d);
        ring.add(2);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {1, 2, 0}, dst, 0.d);
        ring.add(3);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {1, 2, 3}, dst, 0.d);
        ring.add(4);
        ring.toArray(dst, true);
        Assert.assertArrayEquals(new double[] {2, 3, 4}, dst, 0.d);
    }

}
