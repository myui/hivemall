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
package hivemall.utils;

import hivemall.utils.lang.ArrayUtils;

import java.util.Random;

import org.junit.Test;
import org.junit.Assert;

public class ArrayUtilsTest {

    @Test
    public void test() {
        String[] shuffled = new String[] { "1, 2, 3", "4, 5, 6", "7, 8, 9", "10, 11, 12" };
        String[] outcome = new String[] { "10, 11, 12", "1, 2, 3", "4, 5, 6", "7, 8, 9" };

        ArrayUtils.shuffle(shuffled, new Random(0L));

        for(int i = 0; i < shuffled.length; i++) {
            Assert.assertEquals(outcome[i], shuffled[i]);
        }
    }

}
