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
