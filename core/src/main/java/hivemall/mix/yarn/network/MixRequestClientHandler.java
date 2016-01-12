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
package hivemall.mix.yarn.network;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;

public final class MixRequestClientHandler {

    public abstract static class AbstractMixRequestClientHandler extends
            SimpleChannelInboundHandler<MixRequest> {
    }

    @ChannelHandler.Sharable
    public final static class MixRequester extends AbstractMixRequestClientHandler {

        final AtomicReference<String> mixServers;

        public MixRequester(AtomicReference<String> ref) {
            this.mixServers = ref;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixRequest req) throws Exception {
            mixServers.set(req.getAllocatedURIs());
        }
    }

    // TODO: Three classes below are duplicated in MixRequestServerHandler
    public final static class MixRequestInitializer extends ChannelInitializer<SocketChannel> {

        private final AbstractMixRequestClientHandler handler;

        public MixRequestInitializer(AbstractMixRequestClientHandler handler) {
            this.handler = handler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new MixRequestEncoder(), new MixRequestDecoder(), handler);
        }
    }

    private final static class MixRequestDecoder extends MessageToMessageDecoder<ByteBuf> {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                throws Exception {
            int numRequest = in.readInt();
            String URIs = readString(in);
            out.add(new MixRequest(numRequest, URIs));
        }
    }

    public final static class MixRequestEncoder extends MessageToByteEncoder<MixRequest> {

        public MixRequestEncoder() {
            super(MixRequest.class, true);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, MixRequest msg, ByteBuf out)
                throws Exception {
            out.writeInt(msg.getCount());
            writeString(msg.getAllocatedURIs(), out);
        }
    }

    public static void writeString(final String s, final ByteBuf buf)
            throws UnsupportedEncodingException {
        if(s == null) {
            buf.writeInt(-1);
            return;
        }
        byte[] b = s.getBytes("utf-8");
        buf.writeInt(b.length);
        buf.writeBytes(b);
    }

    public static String readString(final ByteBuf in) {
        int length = in.readInt();
        if(length == -1) {
            return null;
        }
        byte[] b = new byte[length];
        in.readBytes(b, 0, length);
        try {
            return new String(b, "utf-8");
        } catch(UnsupportedEncodingException e) {
            return null;
        }
    }
}
