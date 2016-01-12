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
package hivemall.mix.yarn.client;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import hivemall.mix.client.MixRequestRouter;
import hivemall.mix.MixEnv;
import hivemall.mix.yarn.network.MixRequest;
import hivemall.mix.yarn.network.MixRequestClientHandler.MixRequester;
import hivemall.mix.yarn.network.MixRequestClientHandler.MixRequestInitializer;

public final class MixYarnRequestRouter extends MixRequestRouter {

    public MixYarnRequestRouter(String connectInfo) {
        super(connectInfo);
    }

    @Override
    protected String toMixServerList(String connectInfo) {
        // Send a request to AM for allocating MIX servers
        AtomicReference<String> allocatedConnectInfo = new AtomicReference<String>();
        EventLoopGroup workers = new NioEventLoopGroup();
        MixRequester msgHandler = new MixRequester(allocatedConnectInfo);
        Channel ch = null;
        try {
            ch = startNettyClient(new MixRequestInitializer(msgHandler), connectInfo, MixEnv.YARN_RESOURCE_REQUEST_PORT, workers);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        assert ch != null;

        // Block until this MIX server finished
        try {
            ch.writeAndFlush(new MixRequest());
            int retry = 0;
            while(allocatedConnectInfo.get() == null && retry++ < 32) {
                Thread.sleep(500L);
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            workers.shutdownGracefully();
        }

        return allocatedConnectInfo.get();
    }

    private static Channel startNettyClient(ChannelInitializer<SocketChannel> initializer, String host, int port, EventLoopGroup workers)
            throws RuntimeException, InterruptedException {
        Bootstrap b = new Bootstrap();
        b.group(workers);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.handler(initializer);
        SocketAddress remoteAddr = new InetSocketAddress(host, port);
        Channel ch;
        int retry = 0;
        while(true) {
            try {
                ch = b.connect(remoteAddr).sync().channel();
                if(ch.isActive())
                    break;
            } catch (Exception e) {
                // Ignore it
            }
            if(++retry > 8) {
                throw new RuntimeException("Can't connect " + host + ":" + Integer.toString(port));
            }
            // If inactive, retry it
            Thread.sleep(500L);
        }
        return ch;
    }

}
