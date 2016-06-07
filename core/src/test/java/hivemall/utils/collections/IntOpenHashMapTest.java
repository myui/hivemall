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
        for (int i = 0; i < numEntries; i++) {
            Assert.assertNull(map.put(i, i));
        }
        Assert.assertEquals(numEntries, map.size());
        for (int i = 0; i < numEntries; i++) {
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
        for (int i = 0; i < numEntries; i++) {
            Assert.assertNull(map.put(i, i));
        }
        Assert.assertEquals(numEntries, map.size());

        itor = map.entries();
        Assert.assertTrue(itor.hasNext());
        while (itor.hasNext()) {
            Assert.assertFalse(itor.next() == -1);
            int k = itor.getKey();
            Integer v = itor.getValue();
            Assert.assertEquals(k, v.intValue());
        }
        Assert.assertEquals(-1, itor.next());
    }

}
