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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.hadoop.yarn.api.records.NodeId;

import hivemall.mix.yarn.MixYarnEnv;
import hivemall.mix.yarn.utils.TimestampedValue;

public final class MixRequestServerHandler {

    public abstract static class AbstractMixRequestServerHandler extends
            SimpleChannelInboundHandler<MixRequest> {
    }

    @ChannelHandler.Sharable
    public final static class MixRequestReceiver extends AbstractMixRequestServerHandler {

        final Map<String, TimestampedValue<NodeId>> activeMixServers;

        public MixRequestReceiver(Map<String, TimestampedValue<NodeId>> nodes) {
            this.activeMixServers = nodes;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixRequest req)
                throws Exception {
            /**
             * TODO: In this initial implementation, this function returns all the active MIX
             * servers. In a future, it could return a subset of the servers
             * while considering load balancing.
             */
            final List<String> keys = new ArrayList<String>(activeMixServers.keySet());
            final List<String> urls = new ArrayList<String>();
            for(String key : keys) {
                final TimestampedValue<NodeId> node = activeMixServers.get(key);
                if(node == null || node.getValue().getPort() == -1) {
                    continue;
                }
                urls.add(node.toString());
            }
            MixRequest msg = new MixRequest(urls.size(), join(MixYarnEnv.MIXSERVER_SEPARATOR, urls));
            ctx.writeAndFlush(msg);
        }

        private static String join(String sep, Iterable<String> elements) {
            StringBuilder sb = new StringBuilder();
            for(String e : elements) {
                if(e != null) {
                    if(sb.length() > 0) {
                        sb.append(sep);
                    }
                    sb.append(e);
                }
            }
            return sb.toString();
        }
    }

    public final static class MixRequestInitializer extends ChannelInitializer<SocketChannel> {

        private final AbstractMixRequestServerHandler handler;

        public MixRequestInitializer(AbstractMixRequestServerHandler handler) {
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
            String URIs = NettyUtils.readString(in);
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
            NettyUtils.writeString(msg.getAllocatedURIs(), out);
        }
    }
}
