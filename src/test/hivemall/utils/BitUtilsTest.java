/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.utils;

import hivemall.utils.lang.BitUtils;

import java.util.BitSet;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

public class BitUtilsTest {

    @Test
    public void test() {
        final Random rand = new Random(31);
        for(int i = 0; i < 10000; i++) {
            int r = rand.nextInt(Integer.MAX_VALUE);
            String expected = Integer.toBinaryString(r);
            BitSet b = BitUtils.toBitSet(expected);
            String actual = BitUtils.toBinaryString(b);
            Assert.assertEquals(expected, actual);
        }
    }

}
