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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import hivemall.mix.server.MixServer;
import hivemall.mix.yarn.MixYarnEnv;
import hivemall.mix.yarn.network.NettyUtils;
import hivemall.mix.yarn.network.HeartbeatHandler.HeartbeatReporter;
import hivemall.mix.yarn.network.HeartbeatHandler.HeartbeatReporterInitializer;
import hivemall.utils.lang.CommandLineUtils;

public final class MixYarnServer extends MixServer {

    public MixYarnServer(CommandLine cl) {
        super(cl);
    }

    public static void main(String[] args) {
        // Parse input arguments
        final Options opts = getOptions();
        final CommandLine cl = CommandLineUtils.parseOptions(args, opts);
        final MixServer mixServ = new MixYarnServer(cl);
        final String containerId = cl.getOptionValue("container_id");
        final String appMasterHost = cl.getOptionValue("appmaster_host");

        // Start MixServer
        final ExecutorService mixServExec = Executors.newFixedThreadPool(1);
        Future<?> f = mixServExec.submit(mixServ);

        // Wait until MixServer gets ready
        while(true) {
            try {
                Thread.sleep(500L);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
            if(mixServ.getState() == ServerState.RUNNING) {
                break;
            }
        }

        // Start netty daemon for reporting heartbeats to AM
        EventLoopGroup workers = new NioEventLoopGroup();
        final String host = NettyUtils.getHostAddress();
        final int port = mixServ.getBoundPort();
        HeartbeatReporter msgHandler = new HeartbeatReporter(containerId, host, port);
        try {
            NettyUtils.startNettyClient(new HeartbeatReporterInitializer(msgHandler), appMasterHost, MixYarnEnv.REPORT_RECEIVER_PORT, workers);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        // Block until this MIX server finished
        try {
            f.get();
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            workers.shutdownGracefully();
            mixServExec.shutdown();
        }
    }

    protected static Options getOptions() {
        Options opts = MixServer.getOptions();
        opts.addOption("container_id", true, "Container id of this MIX server assigned by YARN");
        opts.addOption("appmaster_host", true, "Hostname of an application master");
        return opts;
    }
}
