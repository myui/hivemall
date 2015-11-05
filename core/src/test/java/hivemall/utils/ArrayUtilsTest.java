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
