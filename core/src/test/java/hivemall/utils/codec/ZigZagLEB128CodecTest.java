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
package hivemall.utils.codec;

import hivemall.utils.io.FastByteArrayOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class ZigZagLEB128CodecTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testWriteFloat() throws IOException {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        ZigZagLEB128Codec.writeFloat(Float.MAX_VALUE - 0.1f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(Float.MAX_VALUE, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(Float.MIN_VALUE, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(Float.MIN_VALUE + 0.1f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(Float.MIN_NORMAL + 0.1f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(0.01f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(-0.01f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(-100f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(100f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(100.001f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.reset();

        ZigZagLEB128Codec.writeFloat(-100.001f, new DataOutputStream(out));
        Assert.assertTrue("serialized size is greater than 5: " + out.size(), out.size() <= 5);
        out.close();
    }

}
