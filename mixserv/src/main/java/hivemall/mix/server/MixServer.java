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

import hivemall.mix.MixEnv;
import hivemall.mix.metrics.MetricsRegistry;
import hivemall.mix.metrics.MixServerMetrics;
import hivemall.mix.metrics.ThroughputCounter;
import hivemall.mix.store.SessionStore;
import hivemall.mix.store.SessionStore.IdleSessionSweeper;
import hivemall.utils.lang.CommandLineUtils;
import hivemall.utils.lang.Primitives;
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

import java.security.cert.CertificateException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class MixServer implements Runnable {
    private static final Log logger = LogFactory.getLog(MixServer.class);

    private final int port;
    private final int numWorkers;
    private final boolean ssl;
    private final float scale;
    private final short syncThreshold;
    private final long sessionTTLinSec;
    private final long sweepIntervalInSec;
    private final boolean jmx;
    private volatile ServerState state;

    public MixServer(CommandLine cl) {
        this.port = Primitives.parseInt(cl.getOptionValue("port"), MixEnv.MIXSERV_DEFAULT_PORT);
        int procs = Runtime.getRuntime().availableProcessors();
        int workers = Math.max(1, (int) Math.round(procs * 1.5f));
        this.numWorkers = Primitives.parseInt(cl.getOptionValue("num_workers"), workers);
        this.ssl = cl.hasOption("ssl");
        this.scale = Primitives.parseFloat(cl.getOptionValue("scale"), 1.f);
        this.syncThreshold = Primitives.parseShort(cl.getOptionValue("sync"), (short) 30);
        this.sessionTTLinSec = Primitives.parseLong(cl.getOptionValue("ttl"), 120L);
        this.sweepIntervalInSec = Primitives.parseLong(cl.getOptionValue("sweep"), 60L);
        this.jmx = cl.hasOption("jmx");
        this.state = ServerState.INITIALIZING;
        // Print the configurations that this Mix server works with
        logger.info(this.toString());
    }

    public static void main(String[] args) {
        Options opts = getOptions();
        CommandLine cl = CommandLineUtils.parseOptions(args, opts);
        new MixServer(cl).run();
    }

    static Options getOptions() {
        Options opts = new Options();
        opts.addOption("p", "port", true, "port number of the mix server [default: 11212]");
        opts.addOption("workers", "num_workers", true,
            "The number of MIX workers [default: max(1, round(procs * 1.5))] ");
        opts.addOption("ssl", false, "Use SSL for the mix communication [default: false]");
        opts.addOption("scale", "scalemodel", true,
            "Scale values of prediction models to avoid overflow [default: 1.0 (no-scale)]");
        opts.addOption("sync", "sync_threshold", true,
            "Synchronization threshold using clock difference [default: 30]");
        opts.addOption("ttl", "session_ttl", true,
            "The TTL in sec that an idle session lives [default: 120 sec]");
        opts.addOption("sweep", "session_sweep_interval", true,
            "The interval in sec that the session expiry thread runs [default: 60 sec]");
        opts.addOption("jmx", "metrics", false,
            "Toggle this option to enable monitoring metrics using JMX [default: false]");
        return opts;
    }

    @Override
    public String toString() {
        return "[port=" + port + ", numWorkers=" + numWorkers + ", ssl=" + ssl + ", scale=" + scale
                + ", syncThreshold=" + syncThreshold + ", sessionTTLinSec=" + sessionTTLinSec
                + ", sweepIntervalInSec=" + sweepIntervalInSec + ", jmx=" + jmx + ", state="
                + state + "]";
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
        if (ssl) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
        } else {
            sslCtx = null;
        }

        // configure metrics
        ScheduledExecutorService metricCollector = Executors.newScheduledThreadPool(1);
        MixServerMetrics metrics = new MixServerMetrics();
        ThroughputCounter throughputCounter = new ThroughputCounter(metricCollector, 5000L, metrics);
        if (jmx) {// register mbean
            MetricsRegistry.registerMBeans(metrics, port);
        }

        // configure initializer
        SessionStore sessionStore = new SessionStore();
        MixServerHandler msgHandler = new MixServerHandler(sessionStore, syncThreshold, scale);
        MixServerInitializer initializer = new MixServerInitializer(msgHandler, throughputCounter,
            sslCtx);

        Runnable cleanSessionTask = new IdleSessionSweeper(sessionStore, sessionTTLinSec * 1000L);
        ScheduledExecutorService idleSessionChecker = Executors.newScheduledThreadPool(1);
        try {
            // start idle session sweeper
            idleSessionChecker.scheduleAtFixedRate(cleanSessionTask, sessionTTLinSec + 10L,
                sweepIntervalInSec, TimeUnit.SECONDS);
            // accept connections
            acceptConnections(initializer, port, numWorkers);
        } finally {
            // release threads
            idleSessionChecker.shutdownNow();
            if (jmx) {
                MetricsRegistry.unregisterMBeans(port);
            }
            metricCollector.shutdownNow();
        }
    }

    private void acceptConnections(@Nonnull MixServerInitializer initializer, int port,
            @Nonnegative int numWorkers) throws InterruptedException {
        final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final EventLoopGroup workerGroup = new NioEventLoopGroup(numWorkers);
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
