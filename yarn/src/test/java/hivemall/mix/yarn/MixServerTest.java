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
package hivemall.mix.yarn;

import hivemall.mix.yarn.network.MixServerRequest;
import hivemall.mix.yarn.network.MixServerRequestHandler.AbstractMixServerRequestHandler;
import hivemall.mix.yarn.network.MixServerRequestHandler.MixServerRequestInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MixServerTest {
    private static final Log logger = LogFactory.getLog(ApplicationMaster.class);
    private static final String appMasterJar = JarFinder.getJar(ApplicationMaster.class);
    private static final int numNodeManager = 2;

    private MiniYARNCluster yarnCluster;
    private YarnConfiguration conf;

    @Before
    public void setup() throws Exception {
        logger.info("Starting up a YARN cluster");

        conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        conf.set("yarn.log.dir", "target");
        conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
        conf.set(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class.getName());
        String NODE_LABELS_PREFIX = YarnConfiguration.YARN_PREFIX + "node-labels.";
        String NODE_LABELS_ENABLED = NODE_LABELS_PREFIX + "enabled";
        conf.setBoolean(NODE_LABELS_ENABLED, true);

        org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler sched = new org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler();
        System.out.println(sched.toString());

        if(yarnCluster == null) {
            yarnCluster = new MiniYARNCluster(MixServerTest.class.getSimpleName(), 1, numNodeManager, 1, 1);
            yarnCluster.init(conf);
            yarnCluster.start();

            waitForNMsToRegister();

            final URL url = this.getClass().getResource("/yarn-site.xml");
            Assert.assertNotNull("Could not find 'yarn-site.xml' dummy file in classpath", url);

            Configuration yarnClusterConfig = yarnCluster.getConfig();
            yarnClusterConfig.set("yarn.application.classpath", new File(url.getPath()).getParent());
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            yarnClusterConfig.writeXml(bytesOut);
            bytesOut.close();
            OutputStream os = new FileOutputStream(new File(url.getPath()));
            os.write(bytesOut.toByteArray());
            os.close();
        }
    }

    @After
    public void tearDown() throws IOException {
        if(yarnCluster != null) {
            try {
                yarnCluster.stop();
            } finally {
                yarnCluster = null;
            }
        }
    }

    private void waitForNMsToRegister() throws Exception {
        int retry = 0;
        while(true) {
            Thread.sleep(1000L);
            if(yarnCluster.getResourceManager().getRMContext().getRMNodes().size() >= numNodeManager) {
                break;
            }
            if(retry++ > 60) {
                Assert.fail("Can't launch a yarn cluster");
            }
        }
    }

    @Test
    public void testSimpleScenario() throws Exception {
        int numMixServers = 1;
        String[] args = { "--jar", appMasterJar, "--num_containers",
                Integer.toString(numMixServers), "--master_memory", "128", "--master_vcores", "1",
                "--container_memory", "128", "--container_vcores", "1" };

        final MixServerRunner mixClusterRunner = new MixServerRunner(new Configuration(yarnCluster.getConfig()));
        boolean initSuccess = mixClusterRunner.init(args);
        Assert.assertTrue(initSuccess);

        final AtomicBoolean result = new AtomicBoolean(false);
        ExecutorService mixExec = Executors.newSingleThreadExecutor();
        Future<?> mixCluster = mixExec.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    result.set(mixClusterRunner.run());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // Check if ApplicationMaster works correctly
        YarnClient yarnClient = YarnClient.createYarnClient();

        yarnClient.init(new Configuration(yarnCluster.getConfig()));
        yarnClient.start();

        while(true) {
            List<ApplicationReport> apps = yarnClient.getApplications();
            if(apps.size() == 0) {
                Thread.sleep(500L);
                continue;
            }
            Assert.assertEquals(1, apps.size());
            ApplicationReport appReport = apps.get(0);
            if(appReport.getHost().equals("N/A")) {
                Thread.sleep(100L);
                continue;
            }
            Assert.assertEquals(YarnApplicationState.RUNNING, appReport.getYarnApplicationState());
            break;
        }

        // TODO: How to wait until a netty server
        // for resource requests is active.
        Thread.sleep(1000L);

        // Resource allocated from ApplicationMaster
        AtomicReference<String> mixServers = new AtomicReference<String>();

        EventLoopGroup workers = new NioEventLoopGroup();
        MixServerRequester msgHandler = new MixServerRequester(mixServers);
        Channel ch = startNettyClient(new MixServerRequestInitializer(msgHandler), MixYarnEnv.RESOURCE_REQUEST_PORT, workers);

        // Request all the MIX servers
        ch.writeAndFlush(new MixServerRequest()).sync();
        int retry = 0;
        while(mixServers.get() == null && retry++ < 32) {
            Thread.sleep(500L);
        }

        Assert.assertNotNull(mixServers.get());

        verifyContainerErrLog(numMixServers, "REGISTERED");
        verifyContainerErrLog(numMixServers, "ACTIVE");

        // Parse allocated MIX servers
        String[] hosts = mixServers.get().split(Pattern.quote(MixYarnEnv.MIXSERVER_SEPARATOR));
        Assert.assertEquals(hosts.length, 1);

        // TODO: Issue shutdown requests to MIX servers
        mixClusterRunner.forceKillApplication();

        mixExec.shutdown();
        mixCluster.get();
        Assert.assertTrue(result.get());
        workers.shutdownGracefully();
    }

    private static Channel startNettyClient(MixServerRequestInitializer initializer, int port, EventLoopGroup workers)
            throws InterruptedException {
        Bootstrap b = new Bootstrap();
        b.group(workers);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.option(ChannelOption.TCP_NODELAY, true);
        b.handler(initializer);
        SocketAddress remoteAddr = new InetSocketAddress("localhost", port);
        Channel ch = null;
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
                Assert.fail("Failed to connect to ApplicationMaster");
                break;
            }
            // If inactive, retry it
            Thread.sleep(500L);
        }
        return ch;
    }

    @ChannelHandler.Sharable
    public final class MixServerRequester extends AbstractMixServerRequestHandler {

        final AtomicReference<String> mixServers;

        public MixServerRequester(AtomicReference<String> ref) {
            this.mixServers = ref;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixServerRequest req)
                throws Exception {
            mixServers.set(req.getAllocatedURIs());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    private int verifyContainerErrLog(int numContainer, String expectedWord) {
        int numOfWords = 0;
        for(int i = 0; i < numNodeManager; i++) {
            Configuration config = yarnCluster.getNodeManager(i).getConfig();
            String logDirs = config.get(YarnConfiguration.NM_LOG_DIRS, YarnConfiguration.DEFAULT_NM_LOG_DIRS);
            File logDir = new File(logDirs);
            File[] logFiles = logDir.listFiles();
            logger.info("NodeManager LogDir:" + logDirs + " (#files:" + logFiles.length + ")");
            int logFileIndex = -1;
            for(int j = logFiles.length - 1; j >= 0; j--) {
                if(logFiles[j].listFiles().length == numContainer + 1) {
                    logFileIndex = j;
                    break;
                }
            }
            if(logFileIndex == -1)
                continue;

            File[] containerDirs = logFiles[logFileIndex].listFiles();
            for(File dir : containerDirs) {
                for(File output : dir.listFiles()) {
                    if(output.getName().trim().contains("stderr")) {
                        BufferedReader br = null;
                        try {
                            String sCurrentLine;
                            br = new BufferedReader(new FileReader(output));
                            while((sCurrentLine = br.readLine()) != null) {
                                if(sCurrentLine.contains(expectedWord)) {
                                    numOfWords++;
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            IOUtils.closeQuietly(br);
                        }
                    }
                }
            }
        }
        return numOfWords;
    }
}
