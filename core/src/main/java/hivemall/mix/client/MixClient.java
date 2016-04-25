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
package hivemall.mix.client;

import hivemall.model.ModelUpdateHandler;
import hivemall.mix.MixMessage;
import hivemall.mix.MixMessage.MixEventName;
import hivemall.mix.MixedModel;
import hivemall.mix.MixedWeight;
import hivemall.mix.NodeInfo;
import hivemall.utils.hadoop.HadoopUtils;
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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

public final class MixClient implements ModelUpdateHandler, Closeable {
    public static final String DUMMY_JOB_ID = "__DUMMY_JOB_ID__";

    private final MixEventName event;
    private String groupID;
    private final boolean ssl;
    private final int mixThreshold;
    private final MixRequestRouter router;
    private final MixClientHandler msgHandler;
    private final Map<NodeInfo, Channel> channelMap;

    private boolean initialized = false;
    private EventLoopGroup workers;

    public MixClient(@Nonnull MixEventName event, @CheckForNull String groupID, @Nonnull String connectURIs, boolean ssl, int mixThreshold, @Nonnull MixedModel model) {
        if(groupID == null) {
            throw new IllegalArgumentException("groupID is null");
        }
        if(mixThreshold < 1 || mixThreshold > Byte.MAX_VALUE) {
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

    /**
     * @return true if sent request, otherwise false
     */
    @Override
    public boolean onUpdate(Object feature, float weight, float covar, short clock, int deltaUpdates)
            throws Exception {
        assert (deltaUpdates > 0) : deltaUpdates;
        if(deltaUpdates < mixThreshold) {
            return false; // avoid mixing
        }

        if(!initialized) {
            replaceGroupIDIfRequired();
            initialize(); // initialize connections to mix servers
        }

        MixMessage msg = new MixMessage(event, feature, weight, covar, clock, deltaUpdates);
        msg.setGroupID(groupID);

        NodeInfo server = router.selectNode(msg);
        Channel ch = channelMap.get(server);
        if(!ch.isActive()) {// reconnect
            SocketAddress remoteAddr = server.getSocketAddress();
            ch.connect(remoteAddr).sync();
        }

        //ch.writeAndFlush(msg).sync();
        ch.writeAndFlush(msg); // send asynchronously in the background
        return true;
    }

    @Override
    public void sendCancelRequest(@Nonnull Object feature, @Nonnull MixedWeight mixed)
            throws Exception {
        assert (initialized);

        float weight = mixed.getWeight();
        float covar = mixed.getCovar();
        int deltaUpdates = mixed.getDeltaUpdates();

        MixMessage msg = new MixMessage(event, feature, weight, covar, deltaUpdates, true);
        assert (groupID != null);
        msg.setGroupID(groupID);

        // TODO REVIEWME consider mix server faults (what if mix server dead? Do not send cancel request?)
        NodeInfo server = router.selectNode(msg);
        Channel ch = channelMap.get(server);
        if(!ch.isActive()) {// reconnect
            SocketAddress remoteAddr = server.getSocketAddress();
            ch.connect(remoteAddr).sync();
        }

        ch.writeAndFlush(msg); // send asynchronously in the background
    }

    private void replaceGroupIDIfRequired() {
        if(groupID.startsWith(DUMMY_JOB_ID)) {
            String jobId = HadoopUtils.getJobId();
            this.groupID = groupID.replace(DUMMY_JOB_ID, jobId);
        }
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
