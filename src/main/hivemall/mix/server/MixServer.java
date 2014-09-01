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
package hivemall.mix.server;

import hivemall.utils.lang.CommandLineUtils;
import hivemall.utils.lang.Primitives;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public final class MixServer implements Runnable {
    public static final int DEFAULT_PORT = 11212;

    private final int port;
    private final boolean ssl;
    private final float scale;
    private final short syncThreshold;

    public MixServer(CommandLine cl) {
        this.port = Primitives.parseInt(cl.getOptionValue("port"), DEFAULT_PORT);
        this.ssl = cl.hasOption("ssl");
        this.scale = Primitives.parseFloat(cl.getOptionValue("scale"), 1.f);
        this.syncThreshold = Primitives.parseShort(cl.getOptionValue("sync"), (short) 30);
    }

    public static void main(String[] args) {
        Options opts = getOptions();
        CommandLine cl = CommandLineUtils.parseOptions(args, opts);
        new MixServer(cl).run();
    }

    static Options getOptions() {
        Options opts = new Options();
        opts.addOption("p", "port", true, "port number of the mix server [default: 11212]");
        opts.addOption("ssl", false, "Use SSL for the mix communication [default: false]");
        opts.addOption("scale", "scalemodel", true, "Scale values of prediction models to avoid overflow [default: 1.0 (no-scale)]");
        opts.addOption("sync", "sync_threshold", true, "Synchronization threshold using clock difference [default: 30]");
        return opts;
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
        if(ssl) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
        } else {
            sslCtx = null;
        }

        MixServerHandler msgHandler = new MixServerHandler(syncThreshold, scale);
        MixServerInitializer initializer = new MixServerInitializer(msgHandler, sslCtx);

        final EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup);
            b.channel(NioServerSocketChannel.class);
            b.handler(new LoggingHandler(LogLevel.INFO));
            b.childHandler(initializer);

            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(port).sync();

            // Wait until the server socket is closed.
            // In this example, this does not happen, but you can do that to gracefully
            // shut down your server.
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

}
