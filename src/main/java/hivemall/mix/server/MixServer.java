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
package hivemall.mix.server;

import hivemall.mix.metrics.MetricsRegistry;
import hivemall.mix.metrics.MixServerMetrics;
import hivemall.mix.metrics.ThroughputCounter;
import hivemall.mix.store.SessionStore;
import hivemall.mix.store.SessionStore.IdleSessionSweeper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.commons.cli.CommandLine;

import java.security.cert.CertificateException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

public final class MixServer implements Runnable {
    public static final int DEFAULT_PORT = 11212;

    private final MixServerArguments options;

    private volatile ServerState state;

    public MixServer(String[] args) {
        this.options = new MixServerArguments(args);
    }

    public MixServer(CommandLine cl) {
        this.options = new MixServerArguments(cl);
    }

    public static void main(String[] args) {
        new MixServer(args).run();
    }

    public MixServerArguments getOptions() {
        return options;
    }

    public ServerState getState() {
        return state;
    }

    @Override
    public void run() {
        try {
            start();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (SSLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void start() throws CertificateException, SSLException, InterruptedException {
        // Configure SSL.
        final SslContext sslCtx;
        if(options.isSsl()) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
        } else {
            sslCtx = null;
        }

        // configure metrics
        ScheduledExecutorService metricCollector = Executors.newScheduledThreadPool(1);
        MixServerMetrics metrics = new MixServerMetrics();
        ThroughputCounter throughputCounter = new ThroughputCounter(metricCollector, 5000L, metrics);
        if(options.isJmx()) {// register mbean
            MetricsRegistry.registerMBeans(metrics, options.getPort());
        }

        // configure initializer
        SessionStore sessionStore = new SessionStore();
        MixServerHandler msgHandler = new MixServerHandler(
                sessionStore,
                options.getSyncThreshold(),
                options.getScale()
        );
        MixServerInitializer initializer = new MixServerInitializer(
                msgHandler, throughputCounter, sslCtx);

        Runnable cleanSessionTask = new IdleSessionSweeper(
                sessionStore,
                options.getSessionTTLinSec() * 1000L
        );
        ScheduledExecutorService idleSessionChecker = Executors.newScheduledThreadPool(1);
        try {
            // start idle session sweeper
            idleSessionChecker.scheduleAtFixedRate(
                    cleanSessionTask,
                    options.getSessionTTLinSec() + 10L,
                    options.getSweepIntervalInSec(),
                    TimeUnit.SECONDS
            );
            // accept connections
            acceptConnections(initializer, options.getPort());
        } finally {
            // release threads
            idleSessionChecker.shutdownNow();
            if(options.isJmx()) {
                MetricsRegistry.unregisterMBeans(options.getPort());
            }
            metricCollector.shutdownNow();
        }
    }

    private void acceptConnections(@Nonnull MixServerInitializer initializer, int port)
            throws InterruptedException {
        final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final EventLoopGroup workerGroup = new NioEventLoopGroup(options.getNthreads());
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.group(bossGroup, workerGroup);
            b.channel(NioServerSocketChannel.class);
            b.handler(new LoggingHandler(LogLevel.INFO));
            b.childHandler(initializer);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync();
            this.state = ServerState.RUNNING;

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } finally {
            this.state = ServerState.STOPPING;
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public enum ServerState {
        INITIALIZING, RUNNING, STOPPING,
    }

}
