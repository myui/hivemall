/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.utils.compress;

import hivemall.utils.io.IOUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;

public class Base91Test {

    @Test
    public void testSimpleEncodeDecode() throws UnsupportedEncodingException {
        byte[] expected1 = "This is a test".getBytes();
        byte[] encoded1 = Base91.encode(expected1);
        String actualString1 = new String(encoded1, "UTF-8");
        Assert.assertEquals("nX,;/WRT%yxth90oZB", actualString1);
        byte[] actual1 = Base91.decode(encoded1);
        Assert.assertArrayEquals(expected1, actual1);
        byte[] expected2 = "".getBytes();
        byte[] actual2 = Base91.decode(Base91.encode(expected2));
        Assert.assertArrayEquals(expected2, actual2);
    }


    @Test
    public void testLongEncodeDecode() throws IOException {
        byte[] expected1 = IOUtils.toString(
            Base91Test.class.getResourceAsStream("Base91Test.class")).getBytes();
        Assert.assertTrue(expected1.length > 1000);
        byte[] actual1 = Base91.decode(Base91.encode(expected1));
        Assert.assertArrayEquals(expected1, actual1);
    }


}
