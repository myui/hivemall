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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import hivemall.mix.NodeInfo;
import hivemall.mix.yarn.client.MixYarnRequestRouter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
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
import org.junit.*;

import hivemall.mix.yarn.network.NettyUtils;
import hivemall.mix.yarn.network.MixRequest;
import hivemall.mix.yarn.network.MixRequestServerHandler.AbstractMixRequestServerHandler;
import hivemall.mix.yarn.network.MixRequestServerHandler.MixRequestInitializer;

public final class MixClusterTest {
    private static final Log logger = LogFactory.getLog(MixClusterTest.class);
    private static YarnConfiguration conf = new YarnConfiguration();
    private static final String appJar = JarFinder.getJar(ApplicationMaster.class);
    private static final int numNodeManager = 2;

    private MiniYARNCluster yarnCluster;
    private MixClusterRunner mixClusterRunner;
    private ExecutorService mixClusterExec;

    @BeforeClass
    public static void setupOnce() {
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 128);
        conf.set("yarn.log.dir", "target");
    }

    @Before
    public void setup() throws Exception {
        assert yarnCluster == null;

        logger.info("Starting up a YARN cluster");
        this.yarnCluster = new MiniYARNCluster(MixClusterTest.class.getSimpleName(), 1, numNodeManager, 1, 1);
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

        // For a MIX cluster
        this.mixClusterExec = Executors.newSingleThreadExecutor();
    }

    Future<Boolean> startMixCluster(Class<?> mainClass, String[] options) throws Exception {
        assert mixClusterRunner == null;

        this.mixClusterRunner = new MixClusterRunner(mainClass.getName(), new Configuration(yarnCluster.getConfig()));
        Assert.assertTrue(mixClusterRunner.init(options));

        Future<Boolean> mixCluster = mixClusterExec.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                boolean result = false;
                try {
                    result = mixClusterRunner.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return result;
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

        // Why this returns empty string?
        // Assert.assertEquals("localhost", mixClusterRunner.getApplicationMasterHost());

        // TODO: How to wait until a netty server
        // for resource requests is active.
        Thread.sleep(10 * 1000L);

        return mixCluster;
    }

    @After
    public void tearDown() throws Exception {
        try {
            // Shut down a MIX cluster, then a YARN cluster
            if(mixClusterRunner != null) {
                mixClusterRunner.forceKillApplication();
            }
            mixClusterExec.shutdown();
            yarnCluster.stop();
        } finally {
            yarnCluster = null;
            mixClusterRunner = null;
            mixClusterExec = null;
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

    @Test(timeout=360*1000L)
    public void testSimpleScenario() throws Exception {
        int numMixServers = 1;
        final String[] options = { "--jar", appJar, "--num_containers", Integer.toString(numMixServers),
                "--master_memory", "128", "--master_vcores", "1", "--container_memory", "128",
                "--container_vcores", "1" };

        Future<Boolean> result = startMixCluster(ApplicationMaster.class, options);
        Assert.assertEquals(2, verifyContainerLog("REGISTERED"));
        Assert.assertEquals(2, verifyContainerLog("ACTIVE"));

        // Resources allocated from ApplicationMaster
        AtomicReference<String> mixServers = new AtomicReference<String>();

        EventLoopGroup workers = new NioEventLoopGroup();
        MixRequester msgHandler = new MixRequester(mixServers);
        Channel ch = NettyUtils.startNettyClient(new MixRequestInitializer(msgHandler), "localhost", MixYarnEnv.RESOURCE_REQUEST_PORT, workers);

        // Request all the MIX servers
        ch.writeAndFlush(new MixRequest());
        int retry = 0;
        while(mixServers.get() == null && retry++ < 32) {
            Thread.sleep(500L);
        }

        Assert.assertNotNull(mixServers.get());

        // Parse allocated MIX servers
        String[] hosts = mixServers.get().split(Pattern.quote(MixYarnEnv.MIXSERVER_SEPARATOR));
        Assert.assertEquals(numMixServers, hosts.length);
        for(String host : hosts) {
            Assert.assertTrue(host.contains(NettyUtils.getHostAddress()));
        }

        workers.shutdownGracefully();

        // Stop the MIX cluster
        mixClusterRunner.forceKillApplication();
        Assert.assertTrue(result.get());
    }

    @Test(timeout=360*1000L)
    public void testMixYarnRequestRouter() throws Exception {
        int numMixServers = 1;
        final String[] options = { "--jar", appJar, "--num_containers", Integer.toString(numMixServers),
                "--master_memory", "128", "--master_vcores", "1", "--container_memory", "128",
                "--container_vcores", "1" };

        Future<Boolean> result = startMixCluster(ApplicationMaster.class, options);
        Assert.assertEquals(2, verifyContainerLog("REGISTERED"));
        Assert.assertEquals(2, verifyContainerLog("ACTIVE"));

        // Resources allocated from ApplicationMaster
        MixYarnRequestRouter router = new MixYarnRequestRouter("localhost");
        final NodeInfo[] nodes = router.getAllNodes();
        Assert.assertEquals(numMixServers, nodes.length);
        for(NodeInfo node : nodes) {
            Assert.assertEquals(NettyUtils.getHostAddress(), node.getAddress().getHostName());
        }

        // Stop the MIX cluster
        mixClusterRunner.forceKillApplication();
        Assert.assertTrue(result.get());
    }

    @Test(timeout=360*1000L)
    public void testMixServerLaunchFailure() throws Exception {
        final String[] options = { "--jar", appJar, "--num_containers", "1",
                "--master_memory", "128", "--master_vcores", "1", "--container_memory", "128",
                "--container_vcores", "1", "--num_retries", "4" };

        Future<Boolean> result = startMixCluster(RetryDeadMixServerApplicationMaster.class, options);

        // Must be finished with failure status
        Assert.assertFalse(result.get());
        Assert.assertEquals("Total failed count for counters:5", mixClusterRunner.getApplicationMasterDiagnostics());
    }

    @ChannelHandler.Sharable
    public final class MixRequester extends AbstractMixRequestServerHandler {

        final AtomicReference<String> mixServers;

        public MixRequester(AtomicReference<String> ref) {
            this.mixServers = ref;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MixRequest req) throws Exception {
            mixServers.set(req.getAllocatedURIs());
        }
    }

    private int verifyContainerLog(String expectedWord) {
        int numOfWords = 0;
        for(int i = 0; i < numNodeManager; i++) {
            Configuration config = yarnCluster.getNodeManager(i).getConfig();
            String logDirs = config.get(YarnConfiguration.NM_LOG_DIRS, YarnConfiguration.DEFAULT_NM_LOG_DIRS);
            File logDir = new File(logDirs);
            File[] logFiles = logDir.listFiles();
            logger.info("NodeManager LogDir:" + logDirs);
            for(File logs : logFiles) {
                for(File dir : logs.listFiles()) {
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
        }
        return numOfWords;
    }
}
