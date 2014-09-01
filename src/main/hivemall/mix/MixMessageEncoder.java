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
package hivemall.mix;

import hivemall.mix.MixMessage.MixEventName;
import hivemall.utils.lang.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Deprecated
public final class MixMessageEncoder extends MessageToByteEncoder<MixMessage> {
    static final byte INTEGER_TYPE = 1;
    static final byte TEXT_TYPE = 2;
    static final byte STRING_TYPE = 3;
    static final byte INT_WRITABLE_TYPE = 4;
    static final byte LONG_WRITABLE_TYPE = 5;

    public MixMessageEncoder() {
        super(MixMessage.class, true);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, MixMessage msg, ByteBuf out) throws Exception {
        MixEventName event = msg.getEvent();
        byte b = event.getID();
        out.writeByte(b);

        Object feature = msg.getFeature();
        encodeObject(feature, out);

        float weight = msg.getWeight();
        out.writeFloat(weight);

        float covariance = msg.getCovariance();
        out.writeFloat(covariance);

        short clock = msg.getClock();
        out.writeShort(clock);

        int deltaUpdates = msg.getDeltaUpdates();
        out.writeInt(deltaUpdates);

        String groupId = msg.getGroupID();
        writeString(groupId, out);
    }

    private static void encodeObject(final Object obj, final ByteBuf buf) throws IOException {
        assert (obj != null);
        if(obj instanceof Integer) {
            Integer i = (Integer) obj;
            buf.writeByte(INTEGER_TYPE);
            buf.writeInt(i.intValue());
        } else if(obj instanceof Text) {
            Text t = (Text) obj;
            byte[] b = t.getBytes();
            int length = t.getLength();
            buf.writeByte(TEXT_TYPE);
            buf.writeInt(length);
            buf.writeBytes(b, 0, length);
        } else if(obj instanceof String) {
            String s = (String) obj;
            buf.writeByte(STRING_TYPE);
            writeString(s, buf);
        } else if(obj instanceof IntWritable) {
            IntWritable i = (IntWritable) obj;
            buf.writeByte(INT_WRITABLE_TYPE);
            buf.writeInt(i.get());
        } else if(obj instanceof LongWritable) {
            LongWritable l = (LongWritable) obj;
            buf.writeByte(LONG_WRITABLE_TYPE);
            buf.writeLong(l.get());
        } else {
            throw new IllegalStateException("Unexpected type: " + obj.getClass().getName());
        }
    }

    private static void writeString(final String s, final ByteBuf buf) {
        if(s == null) {
            buf.writeInt(-1);
            return;
        }
        byte[] b = StringUtils.getBytes(s);
        int length = b.length;
        buf.writeInt(length);
        buf.writeBytes(b, 0, length);
    }

}
