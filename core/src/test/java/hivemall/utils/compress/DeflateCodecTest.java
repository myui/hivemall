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
package hivemall.utils.compress;

import hivemall.utils.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

public class DeflateCodecTest {

    @Test
    public void testOpscript() throws IOException {
        URL url = new URL("https://raw.githubusercontent.com/myui/hivemall/master/core/pom.xml");
        InputStream is = new BufferedInputStream(url.openStream());
        String opScript = IOUtils.toString(is);
        byte[] original1 = opScript.getBytes();

        DeflateCodec codec = new DeflateCodec();
        byte[] compressed1 = codec.compress(original1);
        byte[] decompressed1 = codec.decompress(compressed1);
        Assert.assertTrue("compressed size (" + compressed1.length + " bytes) < original size ("
                + original1.length + " bytes)", compressed1.length < original1.length);
        Assert.assertArrayEquals(original1, decompressed1);
        codec.close();
    }

    @Test
    public void testNonString() throws IOException {
        DeflateCodec codec = new DeflateCodec();
        byte[] original1 = IOUtils.toString(
            DeflateCodecTest.class.getResourceAsStream("DeflateCodecTest.class")).getBytes();
        byte[] compressed1 = codec.compress(original1);
        byte[] decompressed1 = codec.decompress(compressed1);
        Assert.assertTrue("compressed size (" + compressed1.length + " bytes) < original size ("
                + original1.length + " bytes)", compressed1.length < original1.length);
        Assert.assertArrayEquals(original1, decompressed1);
        codec.close();
    }

    @Test
    public void testReuse() throws IOException {
        DeflateCodec codec = new DeflateCodec();
        byte[] original1 = new String("this is a test").getBytes();
        byte[] compressed1 = codec.compress(original1);
        byte[] decompressed1 = codec.decompress(compressed1);
        Assert.assertArrayEquals(original1, decompressed1);

        compressed1 = codec.compress(original1);
        decompressed1 = codec.decompress(compressed1);
        Assert.assertArrayEquals(original1, decompressed1);
        codec.close();
    }

    @Test
    public void testNoCompression() throws IOException {
        DeflateCodec codec = new DeflateCodec();
        byte[] original0 = new byte[0];
        byte[] compressed0 = codec.compress(original0);
        byte[] decompressed0 = codec.decompress(compressed0);
        Assert.assertEquals(4 + original0.length, compressed0.length);
        Assert.assertArrayEquals(original0, decompressed0);

        byte[] original1 = new byte[] {1};
        byte[] compressed1 = codec.compress(original1);
        byte[] decompressed1 = codec.decompress(compressed1);
        Assert.assertEquals(4 + original1.length, compressed1.length);
        Assert.assertArrayEquals(original1, decompressed1);
        codec.close();
    }

}
