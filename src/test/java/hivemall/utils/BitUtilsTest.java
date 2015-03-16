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
package hivemall.utils;

import hivemall.utils.lang.BitUtils;

import java.util.BitSet;
import java.util.Random;

import org.junit.Test;
import org.junit.Assert;

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
