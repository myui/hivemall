/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.utils.collections;

import org.junit.Assert;
import org.junit.Test;

public class Int2FloatOpenHashMapTest {

    @Test
    public void testSize() {
        Int2FloatOpenHash map = new Int2FloatOpenHash(16384);
        map.put(1, 3.f);
        Assert.assertEquals(3.f, map.get(1), 0.d);
        map.put(1, 5.f);
        Assert.assertEquals(5.f, map.get(1), 0.d);
        Assert.assertEquals(1, map.size());
    }

    @Test
    public void testDefaultReturnValue() {
        Int2FloatOpenHash map = new Int2FloatOpenHash(16384);
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(-1.f, map.get(1), 0.d);
        float ret = Float.MIN_VALUE;
        map.defaultReturnValue(ret);
        Assert.assertEquals(ret, map.get(1), 0.d);
    }

    @Test
    public void testPutAndGet() {
        Int2FloatOpenHash map = new Int2FloatOpenHash(16384);
        final int numEntries = 1000000;
        for(int i = 0; i < numEntries; i++) {
            Assert.assertEquals(-1.f, map.put(i, Float.valueOf(i + 0.1f)), 0.d);
        }
        Assert.assertEquals(numEntries, map.size());
        for(int i = 0; i < numEntries; i++) {
            Float v = map.get(i);
            Assert.assertEquals(i + 0.1f, v.floatValue(), 0.d);
        }
    }

    @Test
    public void testIterator() {
        Int2FloatOpenHash map = new Int2FloatOpenHash(1000);
        Int2FloatOpenHash.IMapIterator itor = map.entries();
        Assert.assertFalse(itor.hasNext());

        final int numEntries = 1000000;
        for(int i = 0; i < numEntries; i++) {
            Assert.assertEquals(-1.f, map.put(i, Float.valueOf(i + 0.1f)), 0.d);
        }
        Assert.assertEquals(numEntries, map.size());

        itor = map.entries();
        Assert.assertTrue(itor.hasNext());
        while(itor.hasNext()) {
            Assert.assertFalse(itor.next() == -1);
            int k = itor.getKey();
            Float v = itor.getValue();
            Assert.assertEquals(k + 0.1f, v.floatValue(), 0.d);
        }
        Assert.assertEquals(-1, itor.next());
    }

}
