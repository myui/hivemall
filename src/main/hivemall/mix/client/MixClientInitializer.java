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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;

public final class MixClientInitializer extends ChannelInitializer<SocketChannel> {

    private final MixClientHandler msgHandler;
    private final SslContext sslCtx;

    public MixClientInitializer(MixClientHandler msgHandler, SslContext sslCtx) {
        if(msgHandler == null) {
            throw new IllegalArgumentException();
        }
        this.msgHandler = msgHandler;
        this.sslCtx = sslCtx;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        if(sslCtx != null) {
            pipeline.addLast(sslCtx.newHandler(ch.alloc()));
        }

        ObjectEncoder encoder = new ObjectEncoder();
        ObjectDecoder decoder = new ObjectDecoder(ClassResolvers.weakCachingConcurrentResolver(null));
        pipeline.addLast(encoder, decoder, msgHandler);
    }

}
