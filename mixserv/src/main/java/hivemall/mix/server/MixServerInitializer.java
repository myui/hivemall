/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.mix.server;

import hivemall.mix.MixMessageDecoder;
import hivemall.mix.MixMessageEncoder;
import hivemall.mix.metrics.ThroughputCounter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class MixServerInitializer extends ChannelInitializer<SocketChannel> {

    @Nonnull
    private final MixServerHandler requestHandler;
    @Nullable
    private final ThroughputCounter throughputCounter;
    @Nullable
    private final SslContext sslCtx;

    public MixServerInitializer(@Nonnull MixServerHandler msgHandler,
            @Nullable ThroughputCounter throughputCounter, @Nullable SslContext sslCtx) {
        this.requestHandler = msgHandler;
        this.throughputCounter = throughputCounter;
        this.sslCtx = sslCtx;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslCtx != null) {
            pipeline.addLast(sslCtx.newHandler(ch.alloc()));
        }

        MixMessageEncoder encoder = new MixMessageEncoder();
        MixMessageDecoder decoder = new MixMessageDecoder();

        if (throughputCounter != null) {
            pipeline.addLast(throughputCounter, decoder, encoder, requestHandler);
        } else {
            pipeline.addLast(decoder, encoder, requestHandler);
        }
    }

}
