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
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@Deprecated
public final class MixMessageDecoder extends ByteToMessageDecoder {

    public MixMessageDecoder() {
        super();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {        
        byte b = in.readByte();
        MixEventName event = MixEventName.resolve(b);
        Object feature = decodeObject(in);
        float weight = in.readFloat();
        float covariance = in.readFloat();
        short clock = in.readShort();
        int deltaUpdates = in.readInt();
        String groupID = readString(in);
        
        MixMessage msg = new MixMessage(event, feature, weight, covariance, clock, deltaUpdates);        
        msg.setGroupID(groupID);
        out.add(msg);        
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
    
}
