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
import java.util.Collections;
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

public final class MixServerRequestHandler {

    public abstract static class AbstractMixServerRequestHandler extends
            SimpleChannelInboundHandler<MixServerRequest> {
    }

    @ChannelHandler.Sharable
    public final static class MixServerRequestReceiver extends AbstractMixServerRequestHandler {

        final Map<String, TimestampedValue<NodeId>> activeMixServers;

        public MixServerRequestReceiver(Map<String, TimestampedValue<NodeId>> nodes) {
            this.activeMixServers = nodes;
        }

        // Visible for testing
        @Override
        public void channelRead0(ChannelHandlerContext ctx, MixServerRequest req)
                throws Exception {
            assert req.getCount() > 0;
            /**
             * TODO: In this initial implementation, this function returns all the active MIX
             * servers. In a future, it could return a subset of the servers
             * while considering load balancing.
             */
            final List<String> keys = new ArrayList<String>(activeMixServers.keySet());
            final List<String> urls = new ArrayList<String>();
            int num = keys.size();
            for(int i = 0; i < num; i++) {
                final TimestampedValue<NodeId> node = activeMixServers.get(keys.get(i));
                if(node == null) {
                    continue;
                }
                urls.add(node.toString());
            }
            MixServerRequest msg = new MixServerRequest(urls.size(), join(MixYarnEnv.MIXSERVER_SEPARATOR, urls));
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

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    public final static class MixServerRequestInitializer extends ChannelInitializer<SocketChannel> {

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
            String URIs = NettyUtils.readString(in);
            out.add(new MixServerRequest(numRequest, URIs));
            in.release();
        }
    }

    public final static class RequestEncoder extends MessageToByteEncoder<MixServerRequest> {

        public RequestEncoder() {
            super(MixServerRequest.class, true);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, MixServerRequest msg, ByteBuf out)
                throws Exception {
            out.writeInt(msg.getCount());
            NettyUtils.writeString(msg.getAllocatedURIs(), out);
        }
    }
}
