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
package hivemall.common;

import hivemall.common.RandomizedAmplifier.DropoutListener;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;
import org.junit.Assert;

public class RandomizedAmplifierTest {

    @Test
    public void test() throws HiveException {
        int size = 10000;
        Integer[] numlist = new Integer[size];
        for (int i = 0; i < size; i++) {
            numlist[i] = i;
        }

        int xtimes = 3;
        RandomizedAmplifier<Integer> amplifier = new RandomizedAmplifier<Integer>(1000, xtimes);
        DropoutCollector collector = new DropoutCollector();
        amplifier.setDropoutListener(collector);
        for (Integer obj : numlist) {
            amplifier.add(obj);
        }
        amplifier.sweepAll();

        Assert.assertEquals(size * xtimes, collector.count);
        Assert.assertEquals(size, collector.numset.size());

        Set<Integer> expectedSet = new HashSet<Integer>(Arrays.asList(numlist));
        Assert.assertEquals(expectedSet, collector.numset);
    }

    private class DropoutCollector implements DropoutListener<Integer> {

        private int count = 0;
        private final Set<Integer> numset = new HashSet<Integer>(10000);

        @Override
        public void onDrop(Integer droppped) {
            //System.out.println(droppped);
            numset.add(droppped);
            count++;
        }

    }

}
