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
package hivemall.mix.yarn.server;

import hivemall.mix.server.MixServer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import hivemall.mix.yarn.network.HeartbeatHandler.HeartbeatReporter;
import hivemall.mix.yarn.network.HeartbeatHandler.HeartbeatReporterInitializer;
import hivemall.utils.lang.CommandLineUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static hivemall.mix.yarn.network.NettyUtils.startNettyClient;

public final class MixYarnServer extends MixServer {

    public MixYarnServer(CommandLine cl) {
        super(cl);
    }

    public static void main(String[] args) {
        // Parse input arguments
        final Options opts = getOptions().addOption("container_id", true, "Container id of this MIX server assigned by YARN");
        final CommandLine cl = CommandLineUtils.parseOptions(args, opts);
        final MixServer mixServ = new MixYarnServer(cl);
        final String containerId = cl.getOptionValue("container_id");
        final String host = getHostAddress();
        final int port = mixServ.getPort();

        // Start netty daemon for reporting heartbeats to AM
        EventLoopGroup workers = new NioEventLoopGroup();
        HeartbeatReporter msgHandler = new HeartbeatReporter(containerId, host, port);
        try {
            startNettyClient(new HeartbeatReporterInitializer(msgHandler), host, port, workers);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Start MixServer
        mixServ.run();

        // Shut down the netty client
        workers.shutdownGracefully();
    }

    private static String getHostAddress() {
        try {
            return InetAddress.getByName("localhost").getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return "";
    }

}
