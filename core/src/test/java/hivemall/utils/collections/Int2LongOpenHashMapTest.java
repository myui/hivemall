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
package hivemall.utils.collections;

import org.junit.Assert;
import org.junit.Test;

public class Int2LongOpenHashMapTest {

    @Test
    public void testSize() {
        Int2LongOpenHashTable map = new Int2LongOpenHashTable(16384);
        map.put(1, 3L);
        Assert.assertEquals(3L, map.get(1));
        map.put(1, 5L);
        Assert.assertEquals(5L, map.get(1));
        Assert.assertEquals(1, map.size());
    }

    @Test
    public void testDefaultReturnValue() {
        Int2LongOpenHashTable map = new Int2LongOpenHashTable(16384);
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(-1L, map.get(1));
        long ret = Long.MIN_VALUE;
        map.defaultReturnValue(ret);
        Assert.assertEquals(ret, map.get(1));
    }

    @Test
    public void testPutAndGet() {
        Int2LongOpenHashTable map = new Int2LongOpenHashTable(16384);
        final int numEntries = 1000000;
        for (int i = 0; i < numEntries; i++) {
            Assert.assertEquals(-1L, map.put(i, i));
        }
        Assert.assertEquals(numEntries, map.size());
        for (int i = 0; i < numEntries; i++) {
            long v = map.get(i);
            Assert.assertEquals(i, v);
        }
    }

    @Test
    public void testIterator() {
        Int2LongOpenHashTable map = new Int2LongOpenHashTable(1000);
        Int2LongOpenHashTable.IMapIterator itor = map.entries();
        Assert.assertFalse(itor.hasNext());

        final int numEntries = 1000000;
        for (int i = 0; i < numEntries; i++) {
            Assert.assertEquals(-1L, map.put(i, i));
        }
        Assert.assertEquals(numEntries, map.size());

        itor = map.entries();
        Assert.assertTrue(itor.hasNext());
        while (itor.hasNext()) {
            Assert.assertFalse(itor.next() == -1);
            int k = itor.getKey();
            long v = itor.getValue();
            Assert.assertEquals(k, v);
        }
        Assert.assertEquals(-1, itor.next());
    }
}
