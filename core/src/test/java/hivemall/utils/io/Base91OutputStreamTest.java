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
package hivemall.utils.io;

import hivemall.fm.ArrayModelTest;
import hivemall.utils.codec.Base91;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;

public class Base91OutputStreamTest {

    @Test
    public void testSmallEncodeOutDecodeIn() throws IOException {
        byte[] expected = "abcdecf".getBytes();

        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        Base91OutputStream base91os = new Base91OutputStream(bos);
        base91os.write(expected);
        base91os.finish();
        byte[] encoded = bos.toByteArray();
        IOUtils.closeQuietly(base91os);
        Assert.assertArrayEquals(Base91.encode(expected), encoded);

        FastByteArrayInputStream bis = new FastByteArrayInputStream(encoded);
        Base91InputStream base91in = new Base91InputStream(bis);

        byte[] actual = IOUtils.toByteArray(base91in);
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testLargeEncodeOutDecodeIn() throws IOException {
        InputStream in = ArrayModelTest.class.getResourceAsStream("bigdata.tr.txt");
        byte[] expected = IOUtils.toByteArray(in);

        FastByteArrayOutputStream bos = new FastByteArrayOutputStream();
        Base91OutputStream base91os = new Base91OutputStream(bos);
        base91os.write(expected);
        base91os.finish();
        byte[] encoded = bos.toByteArray();
        IOUtils.closeQuietly(base91os);

        Assert.assertArrayEquals(Base91.encode(expected), encoded);
        Assert.assertArrayEquals(Base91.decode(encoded), expected);

        FastByteArrayInputStream bis = new FastByteArrayInputStream(encoded);
        Base91InputStream base91in = new Base91InputStream(bis);

        byte[] actual = IOUtils.toByteArray(base91in);
        System.out.println(new String(actual));
        Assert.assertArrayEquals(expected, actual);
    }

}
