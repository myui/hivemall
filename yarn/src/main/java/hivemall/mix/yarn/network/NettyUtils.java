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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public final class NettyUtils {

    public static Channel startNettyClient(ChannelInitializer<SocketChannel> initializer, String host, int port, EventLoopGroup workers)
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

    public static String getHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return "";
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
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }
}
