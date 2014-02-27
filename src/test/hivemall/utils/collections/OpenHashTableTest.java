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

import hivemall.utils.collections.OpenHashTable.IMapIterator;
import junit.framework.Assert;

import org.junit.Test;

public class OpenHashTableTest {

    @Test
    public void testPutAndGet() {
        OpenHashTable<Object, Object> map = new OpenHashTable<Object, Object>(10);
        final int numEntries = 10000;
        for(int i = 0; i < numEntries; i++) {
            map.put(Integer.toString(i), i);
        }
        Assert.assertEquals(numEntries, map.size());
        for(int i = 0; i < numEntries; i++) {
            Object v = map.get(Integer.toString(i));
            Assert.assertEquals(i, v);
        }
    }

    @Test
    public void testIterator() {
        OpenHashTable<String, Integer> map = new OpenHashTable<String, Integer>(100);
        IMapIterator<String, Integer> itor = map.entries();
        Assert.assertFalse(itor.hasNext());

        final int numEntries = 10000;
        for(int i = 0; i < numEntries; i++) {
            map.put(Integer.toString(i), i);
        }

        itor = map.entries();
        Assert.assertTrue(itor.hasNext());
        while(itor.hasNext()) {
            Assert.assertFalse(itor.next() == -1);
            String k = itor.getKey();
            Integer v = itor.getValue();
            Assert.assertEquals(Integer.valueOf(k), v);
        }
        Assert.assertEquals(-1, itor.next());
    }

}
