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
package hivemall.mix.client;

import hivemall.io.ModelUpdateHandler;
import hivemall.io.PredictionModel;
import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.NodeInfo;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLException;

public final class MixClient implements ModelUpdateHandler, Closeable {

    private final MixEventName event;
    private final String groupID;
    private final boolean ssl;
    private final int mixThreshold;
    private final MixRequestRouter router;
    private final MixClientHandler msgHandler;
    private final Map<NodeInfo, Channel> channelMap;

    private boolean initialized = false;
    private EventLoopGroup workers;

    public MixClient(MixEventName event, String groupID, String connectURIs, boolean ssl, int mixThreshold, PredictionModel model) {
        assert (event != null);
        assert (connectURIs != null);
        assert (model != null);
        if(groupID == null) {
            throw new IllegalArgumentException("groupID is null");
        }
        if(mixThreshold < 1) {
            throw new IllegalArgumentException("Invalid mixThreshold: " + mixThreshold);
        }
        this.event = event;
        this.groupID = groupID;
        this.router = new MixRequestRouter(connectURIs);
        this.ssl = ssl;
        this.mixThreshold = mixThreshold;
        this.msgHandler = new MixClientHandler(model);
        this.channelMap = new HashMap<NodeInfo, Channel>();
    }

    private void initialize() throws Exception {
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        NodeInfo[] serverNodes = router.getAllNodes();
        for(NodeInfo node : serverNodes) {
            Bootstrap b = new Bootstrap();
            configureBootstrap(b, workerGroup, node);
        }
        this.workers = workerGroup;
        this.initialized = true;
    }

    private void configureBootstrap(Bootstrap b, EventLoopGroup workerGroup, NodeInfo server)
            throws SSLException, InterruptedException {
        // Configure SSL.
        final SslContext sslCtx;
        if(ssl) {
            sslCtx = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
        } else {
            sslCtx = null;
        }

        b.group(workerGroup);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.channel(NioSocketChannel.class);
        b.handler(new MixClientInitializer(msgHandler, sslCtx));

        SocketAddress remoteAddr = server.getSocketAddress();
        ChannelFuture channelFuture = b.connect(remoteAddr).sync();
        Channel channel = channelFuture.channel();

        channelMap.put(server, channel);
    }

    @Override
    public boolean onUpdate(Object feature, float weight, float covar, short clock)
            throws Exception {
        assert (clock >= 0) : clock;
        if(clock < mixThreshold) {
            return false; // avoid mixing
        }

        if(!initialized) {
            initialize(); // initialize connections to mix servers
        }

        MixMessage msg = new MixMessage(event, feature, weight, covar, clock);
        msg.setGroupID(groupID);

        NodeInfo server = router.selectNode(msg);
        Channel ch = channelMap.get(server);
        if(!ch.isActive()) {// reconnect
            SocketAddress remoteAddr = server.getSocketAddress();
            ch.connect(remoteAddr).sync();
        }

        ch.writeAndFlush(msg);
        return true;
    }

    @Override
    public void close() throws IOException {
        if(workers != null) {
            for(Channel ch : channelMap.values()) {
                ch.close();
            }
            channelMap.clear();
            workers.shutdownGracefully();
            this.workers = null;
        }
    }

}
