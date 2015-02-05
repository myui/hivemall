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

public class IntOpenHashMapTest {

    @Test
    public void testSize() {
        IntOpenHashMap<Float> map = new IntOpenHashMap<Float>(16384);
        map.put(1, Float.valueOf(3.f));
        Assert.assertEquals(Float.valueOf(3.f), map.get(1));
        map.put(1, Float.valueOf(5.f));
        Assert.assertEquals(Float.valueOf(5.f), map.get(1));
        Assert.assertEquals(1, map.size());
    }

    @Test
    public void testPutAndGet() {
        IntOpenHashMap<Integer> map = new IntOpenHashMap<Integer>(16384);
        final int numEntries = 1000000;
        for(int i = 0; i < numEntries; i++) {
            Assert.assertNull(map.put(i, i));
        }
        Assert.assertEquals(numEntries, map.size());
        for(int i = 0; i < numEntries; i++) {
            Integer v = map.get(i);
            Assert.assertEquals(i, v.intValue());
        }
    }

    @Test
    public void testIterator() {
        IntOpenHashMap<Integer> map = new IntOpenHashMap<Integer>(1000);
        IntOpenHashMap.IMapIterator<Integer> itor = map.entries();
        Assert.assertFalse(itor.hasNext());

        final int numEntries = 1000000;
        for(int i = 0; i < numEntries; i++) {
            Assert.assertNull(map.put(i, i));
        }
        Assert.assertEquals(numEntries, map.size());

        itor = map.entries();
        Assert.assertTrue(itor.hasNext());
        while(itor.hasNext()) {
            Assert.assertFalse(itor.next() == -1);
            int k = itor.getKey();
            Integer v = itor.getValue();
            Assert.assertEquals(k, v.intValue());
        }
        Assert.assertEquals(-1, itor.next());
    }

}
