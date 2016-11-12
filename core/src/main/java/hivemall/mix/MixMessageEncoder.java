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

public final class MixMessageEncoder extends MessageToByteEncoder<MixMessage> {
    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

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
        int startIdx = out.writerIndex();
        out.writeBytes(LENGTH_PLACEHOLDER);

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

        boolean cancelRequest = msg.isCancelRequest();
        out.writeBoolean(cancelRequest);

        String groupId = msg.getGroupID();
        writeString(groupId, out);

        int endIdx = out.writerIndex();
        out.setInt(startIdx, endIdx - startIdx - 4);
    }

    private static void encodeObject(final Object obj, final ByteBuf buf) throws IOException {
        assert (obj != null);
        if (obj instanceof Integer) {
            Integer i = (Integer) obj;
            buf.writeByte(INTEGER_TYPE);
            buf.writeInt(i.intValue());
        } else if (obj instanceof Text) {
            Text t = (Text) obj;
            byte[] b = t.getBytes();
            int length = t.getLength();
            buf.writeByte(TEXT_TYPE);
            buf.writeInt(length);
            buf.writeBytes(b, 0, length);
        } else if (obj instanceof String) {
            String s = (String) obj;
            buf.writeByte(STRING_TYPE);
            writeString(s, buf);
        } else if (obj instanceof IntWritable) {
            IntWritable i = (IntWritable) obj;
            buf.writeByte(INT_WRITABLE_TYPE);
            buf.writeInt(i.get());
        } else if (obj instanceof LongWritable) {
            LongWritable l = (LongWritable) obj;
            buf.writeByte(LONG_WRITABLE_TYPE);
            buf.writeLong(l.get());
        } else {
            throw new IllegalStateException("Unexpected type: " + obj.getClass().getName());
        }
    }

    private static void writeString(final String s, final ByteBuf buf) {
        if (s == null) {
            buf.writeInt(-1);
            return;
        }
        byte[] b = StringUtils.getBytes(s);
        int length = b.length;
        buf.writeInt(length);
        buf.writeBytes(b, 0, length);
    }

}
