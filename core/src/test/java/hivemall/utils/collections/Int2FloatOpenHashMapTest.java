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

public class Int2FloatOpenHashMapTest {

    @Test
    public void testSize() {
        Int2FloatOpenHashTable map = new Int2FloatOpenHashTable(16384);
        map.put(1, 3.f);
        Assert.assertEquals(3.f, map.get(1), 0.d);
        map.put(1, 5.f);
        Assert.assertEquals(5.f, map.get(1), 0.d);
        Assert.assertEquals(1, map.size());
    }

    @Test
    public void testDefaultReturnValue() {
        Int2FloatOpenHashTable map = new Int2FloatOpenHashTable(16384);
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(-1.f, map.get(1), 0.d);
        float ret = Float.MIN_VALUE;
        map.defaultReturnValue(ret);
        Assert.assertEquals(ret, map.get(1), 0.d);
    }

    @Test
    public void testPutAndGet() {
        Int2FloatOpenHashTable map = new Int2FloatOpenHashTable(16384);
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
        Int2FloatOpenHashTable map = new Int2FloatOpenHashTable(1000);
        Int2FloatOpenHashTable.IMapIterator itor = map.entries();
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
