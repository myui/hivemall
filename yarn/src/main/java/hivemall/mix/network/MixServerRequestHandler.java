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
package hivemall.mix.network;

import hivemall.mix.yarn.MixYarnEnv;
import hivemall.utils.collections.TimestampedValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public final class MixServerRequestHandler {

    public abstract static class AbstractMixServerRequestHandler
            extends SimpleChannelInboundHandler<MixServerRequest> {}

    @ChannelHandler.Sharable
    public final static class MixServerRequestReceiver extends AbstractMixServerRequestHandler {

        final ConcurrentMap<ContainerId, TimestampedValue<NodeId>> activeMixServers;

        public MixServerRequestReceiver(
                ConcurrentMap<ContainerId, TimestampedValue<NodeId>> nodes) {
            this.activeMixServers = nodes;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixServerRequest req)
                throws Exception {
            // TODO: Return only # of requested resources
            int numServers = 0;
            List<String> urls = new ArrayList<String>();
            for (TimestampedValue<NodeId> value: activeMixServers.values()) {
                final NodeId node = value.getValue();
                urls.add(node.toString());
            }
            MixServerRequest msg =
                    new MixServerRequest(numServers, join(MixYarnEnv.MIXSERVER_SEPARATOR, urls));
            ctx.writeAndFlush(msg);
        }

        private static String join(String sep, Iterable<String> elements) {
            StringBuilder sb = new StringBuilder();
            for (String e : elements) {
                if (e != null) {
                    if (sb.length() > 0) {
                        sb.append(sep);
                    }
                    sb.append(e);
                }
            }
            return sb.toString();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    public final static class MixServerRequestInitializer
            extends ChannelInitializer<SocketChannel> {

        private final AbstractMixServerRequestHandler handler;

        public MixServerRequestInitializer(AbstractMixServerRequestHandler handler) {
            this.handler = handler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new RequestEncoder(), new RequestDecoder(), handler);
        }
    }

    private final static class RequestDecoder extends MessageToMessageDecoder<ByteBuf> {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                throws Exception {
            int numRequest = in.readInt();
            String URIs = readString(in);
            out.add(new MixServerRequest(numRequest, URIs));
        }

        private String readString(final ByteBuf in)
                throws UnsupportedEncodingException {
            int length = in.readInt();
            if(length == -1) {
                return null;
            }
            byte[] b = new byte[length];
            in.readBytes(b, 0, length);
            return new String(b, "utf-8");
        }
    }

    public final static class RequestEncoder extends MessageToByteEncoder<MixServerRequest> {

        public RequestEncoder() {
            super(MixServerRequest.class, true);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, MixServerRequest msg, ByteBuf out)
                throws Exception {
            out.writeInt(msg.getNumRequest());
            writeString(msg.getAllocatedURIs(), out);
        }

        private void writeString(final String s, final ByteBuf buf)
                throws UnsupportedEncodingException {
            if (s == null) {
                buf.writeInt(-1);
                return;
            }
            byte[] b = s.getBytes("utf-8");
            buf.writeInt(b.length);
            buf.writeBytes(b);
        }
    }
}
