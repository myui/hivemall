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

import hivemall.utils.lang.mutable.MutableInt;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class OpenHashMapTest {

    @Test
    public void testPutAndGet() {
        Map<Object, Object> map = new OpenHashMap<Object, Object>(16384);
        final int numEntries = 5000000;
        for (int i = 0; i < numEntries; i++) {
            map.put(Integer.toString(i), i);
        }
        Assert.assertEquals(numEntries, map.size());
        for (int i = 0; i < numEntries; i++) {
            Object v = map.get(Integer.toString(i));
            Assert.assertEquals(i, v);
        }
        map.put(Integer.toString(1), Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, map.get(Integer.toString(1)));
        Assert.assertEquals(numEntries, map.size());
    }

    @Test
    public void testIterator() {
        OpenHashMap<String, Integer> map = new OpenHashMap<String, Integer>(1000);
        IMapIterator<String, Integer> itor = map.entries();
        Assert.assertFalse(itor.hasNext());

        final int numEntries = 1000000;
        for (int i = 0; i < numEntries; i++) {
            map.put(Integer.toString(i), i);
        }

        itor = map.entries();
        Assert.assertTrue(itor.hasNext());
        while (itor.hasNext()) {
            Assert.assertFalse(itor.next() == -1);
            String k = itor.getKey();
            Integer v = itor.getValue();
            Assert.assertEquals(Integer.valueOf(k), v);
        }
        Assert.assertEquals(-1, itor.next());
    }

    @Test
    public void testIteratorGetProbe() {
        OpenHashMap<String, MutableInt> map = new OpenHashMap<String, MutableInt>(100);
        IMapIterator<String, MutableInt> itor = map.entries();
        Assert.assertFalse(itor.hasNext());

        final int numEntries = 1000000;
        for (int i = 0; i < numEntries; i++) {
            map.put(Integer.toString(i), new MutableInt(i));
        }

        final MutableInt probe = new MutableInt();
        itor = map.entries();
        Assert.assertTrue(itor.hasNext());
        while (itor.hasNext()) {
            Assert.assertFalse(itor.next() == -1);
            String k = itor.getKey();
            itor.getValue(probe);
            Assert.assertEquals(Integer.valueOf(k).intValue(), probe.intValue());
        }
        Assert.assertEquals(-1, itor.next());
    }
}
