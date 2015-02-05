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

import static hivemall.mix.MixMessageEncoder.INTEGER_TYPE;
import static hivemall.mix.MixMessageEncoder.INT_WRITABLE_TYPE;
import static hivemall.mix.MixMessageEncoder.LONG_WRITABLE_TYPE;
import static hivemall.mix.MixMessageEncoder.STRING_TYPE;
import static hivemall.mix.MixMessageEncoder.TEXT_TYPE;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.utils.lang.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public final class MixMessageDecoder extends LengthFieldBasedFrameDecoder {

    public MixMessageDecoder() {
        super(1048576/* 1MiB */, 0, 4, 0, 4);
    }

    @Override
    protected MixMessage decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if(frame == null) {
            return null;
        }

        byte b = frame.readByte();
        MixEventName event = MixEventName.resolve(b);
        Object feature = decodeObject(frame);
        float weight = frame.readFloat();
        float covariance = frame.readFloat();
        short clock = frame.readShort();
        int deltaUpdates = frame.readInt();
        boolean cancelRequest = frame.readBoolean();
        String groupID = readString(frame);

        MixMessage msg = new MixMessage(event, feature, weight, covariance, clock, deltaUpdates, cancelRequest);
        msg.setGroupID(groupID);
        return msg;
    }

    private static Object decodeObject(final ByteBuf in) throws IOException {
        final byte type = in.readByte();
        switch(type) {
            case INTEGER_TYPE: {
                int i = in.readInt();
                return Integer.valueOf(i);
            }
            case TEXT_TYPE: {
                int length = in.readInt();
                byte[] b = new byte[length];
                in.readBytes(b, 0, length);
                Text t = new Text(b);
                return t;
            }
            case STRING_TYPE: {
                return readString(in);
            }
            case INT_WRITABLE_TYPE: {
                int i = in.readInt();
                return new IntWritable(i);
            }
            case LONG_WRITABLE_TYPE: {
                long l = in.readLong();
                return new LongWritable(l);
            }
            default:
                break;
        }
        throw new IllegalStateException("Illegal type: " + type);
    }

    private static String readString(final ByteBuf in) {
        int length = in.readInt();
        if(length == -1) {
            return null;
        }
        byte[] b = new byte[length];
        in.readBytes(b, 0, length);
        String s = StringUtils.toString(b);
        return s;
    }

    @Override
    protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.slice(index, length);
    }

}