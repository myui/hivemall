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

import java.util.concurrent.ConcurrentMap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import hivemall.mix.yarn.MixYarnEnv;
import hivemall.mix.yarn.utils.TimestampedValue;

public final class HeartbeatHandler {
    private static final Log logger = LogFactory.getLog(HeartbeatHandler.class);

    @ChannelHandler.Sharable
    public final static class HeartbeatReceiver extends SimpleChannelInboundHandler<Heartbeat> {

        final ConcurrentMap<String, TimestampedValue<NodeId>> activeMixServers;

        public HeartbeatReceiver(ConcurrentMap<String, TimestampedValue<NodeId>> nodes) {
            this.activeMixServers = nodes;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Heartbeat msg) throws Exception {
            logger.info(msg);
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

    public final static class HeartbeatReceiverInitializer extends ChannelInitializer<SocketChannel> {

        private final HeartbeatReceiver handler;

        public HeartbeatReceiverInitializer(HeartbeatReceiver handler) {
            this.handler = handler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new HeartbeatDecoder(), handler);
        }
    }

    @ChannelHandler.Sharable
    public final static class HeartbeatReporter extends SimpleChannelInboundHandler<Heartbeat> {
        private final String containerId;
        private final String host;
        private final int port;

        public HeartbeatReporter(String containerId, String host, int port) {
            this.containerId = containerId;
            this.host = host;
            this.port = port;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Heartbeat msg) throws Exception {
            throw new NotImplementedException();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if(evt instanceof IdleStateEvent) {
                assert ((IdleStateEvent) evt).state() == IdleState.WRITER_IDLE;
                ctx.writeAndFlush(new Heartbeat(containerId, host, port));
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    public final static class HeartbeatReporterInitializer extends ChannelInitializer<SocketChannel> {

        private final HeartbeatReporter handler;

        public HeartbeatReporterInitializer(HeartbeatReporter handler) {
            this.handler = handler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new IdleStateHandler(0, MixYarnEnv.MIXSERVER_HEARTBEAT_INTERVAL, 0));
            pipeline.addLast(new HeartbeatEncoder(), handler);
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
            String containerId = NettyUtils.readString(frame);
            String host = NettyUtils.readString(frame);
            int port = frame.readInt();
            // in.release();
            return new Heartbeat(containerId, host, port);
        }
    }

    public final static class HeartbeatEncoder extends MessageToByteEncoder<Heartbeat> {
        private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

        public HeartbeatEncoder() {
            super(Heartbeat.class, true);
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, Heartbeat msg, ByteBuf out)
                throws Exception {
            int startIdx = out.writerIndex();
            out.writeBytes(LENGTH_PLACEHOLDER);
            NettyUtils.writeString(msg.getConainerId(), out);
            NettyUtils.writeString(msg.getHost(), out);
            out.writeInt(msg.getPort());
            out.setInt(startIdx, out.writerIndex() - startIdx - 4);
        }
    }
}
