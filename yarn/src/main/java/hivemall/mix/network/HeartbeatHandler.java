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

import hivemall.utils.TimestampedValue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.NodeId;

public final class HeartbeatHandler {

    @ChannelHandler.Sharable
    public final static class HeartbeatReceiver extends SimpleChannelInboundHandler<Heartbeat> {

        final ConcurrentMap<String, TimestampedValue<NodeId>> activeMixServers;

        public HeartbeatReceiver(ConcurrentMap<String, TimestampedValue<NodeId>> nodes) {
            this.activeMixServers = nodes;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Heartbeat msg) throws Exception {
            final String containerId = msg.getConainerId();
            final NodeId node = NodeId.newInstance(msg.getHost(), msg.getPort());
            if(activeMixServers.replace(containerId, new TimestampedValue<NodeId>(node)) == null) {
                // If the value does not exist, the MIX server
                // already has gone.
                activeMixServers.remove(containerId);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    public final static class HeartbeatInitializer extends ChannelInitializer<SocketChannel> {

        private final HeartbeatReceiver handler;

        public HeartbeatInitializer(HeartbeatReceiver handler) {
            this.handler = handler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new HeartbeatDecoder(), handler);
        }
    }

    private final static class HeartbeatDecoder extends LengthFieldBasedFrameDecoder {

        public HeartbeatDecoder() {
            super(65536, 0, 4, 0, 4);
        }

        @Override
        protected Heartbeat decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            final ByteBuf frame = (ByteBuf) super.decode(ctx, in);
            if(frame == null) {
                return null;
            }
            String containerId = readString(frame);
            String host = readString(frame);
            int port = frame.readInt();
            return new Heartbeat(containerId, host, port);
        }

        private String readString(final ByteBuf in) {
            int length = in.readInt();
            if(length == -1) {
                return null;
            }
            byte[] b = new byte[length];
            in.readBytes(b, 0, length);
            try {
                return new String(b, "utf-8");
            } catch (UnsupportedEncodingException e) {
                return null;
            }
        }
    }
}
