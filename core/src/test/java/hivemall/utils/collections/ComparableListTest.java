/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class ComparableListTest {
    @Test
    public void testUniqueListInSet() {
        ComparableList<String> l1 = new ComparableList<String>();
        l1.add("A");
        l1.add("B");
        l1.add("C");
        ComparableList<String> l2 = new ComparableList<String>();
        l2.add("A");
        l2.add("B");
        l2.add("C");

        Set<ComparableList<String>> s = new HashSet<ComparableList<String>>();
        s.add(l1);
        s.add(l2);

        Assert.assertEquals(1, s.size());
    }

    @Test
    public void testSortedListInSet() {
        ComparableList<String> l1 = new ComparableList<String>();
        l1.add("A");
        l1.add("B");
        l1.add("C");
        ComparableList<String> l2 = new ComparableList<String>();
        l2.add("A");
        l2.add("B");
        l2.add("D");

        Set<ComparableList<String>> s = new TreeSet<ComparableList<String>>();
        s.add(l1);
        s.add(l2);

        Assert.assertEquals(2, s.size());
    }
}
