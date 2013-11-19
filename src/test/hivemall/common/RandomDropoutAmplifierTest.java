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
package hivemall.common;

import hivemall.common.RandomDropoutAmplifier.DropoutListener;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

public class RandomDropoutAmplifierTest {

    @Test
    public void test() throws HiveException {
        int size = 10000;
        Integer[] numlist = new Integer[size];
        for(int i = 0; i < size; i++) {
            numlist[i] = i;
        }

        int xtimes = 3;
        RandomDropoutAmplifier<Integer> amplifier = new RandomDropoutAmplifier<Integer>(1000, xtimes);
        DropoutCollector collector = new DropoutCollector();
        amplifier.setDropoutListener(collector);
        for(Integer obj : numlist) {
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
