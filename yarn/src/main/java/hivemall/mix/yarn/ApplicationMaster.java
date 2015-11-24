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

import hivemall.mix.launcher.WorkerCommandBuilder;
import hivemall.mix.network.HeartbeatHandler.HeartbeatReceiver;
import hivemall.mix.network.HeartbeatHandler.HeartbeatInitializer;
import hivemall.mix.network.MixServerRequestHandler.MixServerRequestReceiver;
import hivemall.mix.network.MixServerRequestHandler.MixServerRequestInitializer;
import hivemall.utils.StringUtils;
import hivemall.utils.collections.TimestampedValue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.apache.log4j.LogManager;
import io.netty.handler.logging.LogLevel;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class ApplicationMaster {
    private static final Log logger = LogFactory.getLog(ApplicationMaster.class);

    private final String containerMainClass;
    private final Options opts;
    private final Configuration conf;

    // Application Attempt Id (combination of attemptId and fail count)
    private ApplicationAttemptId appAttemptID;

    // Variables passed from MixServerRunner
    private String sharedDir;
    private String mixServJar;

    // Handle to communicate with RM/NM
    private AMRMClientAsync amRMClientAsync;
    private NMClientAsync nmClientAsync;
    private NMCallbackHandler containerListener;

    // Resource parameters for containers
    private int containerMemory;
    private int containerVCores;
    private int numContainers;
    private int requestPriority;
    private int numRetryForFailedContainers;

    // List of allocated containers and alive MIX servers
    private final ConcurrentMap<ContainerId, Container> allocContainers =
            new ConcurrentHashMap<ContainerId, Container>();
    private final ConcurrentMap<ContainerId, TimestampedValue<NodeId>> activeMixServers =
            new ConcurrentHashMap<ContainerId, TimestampedValue<NodeId>>();

    // Info. to launch containers
    private ContainerLaunchInfo cmdInfo = new ContainerLaunchInfo();

    // Thread pool for container launchers
    private final ExecutorService containerExecutor =
            Executors.newFixedThreadPool(1);

    // Check if MIX servers keep alive
    private final ScheduledExecutorService monitorContainerExecutor =
            Executors.newScheduledThreadPool(1);

    // Group for netty workers
    private final Set<EventLoopGroup> nettyWorkers =
            new HashSet<EventLoopGroup>();

    // Trackers for container status
    private final AtomicInteger numAllocatedContainers = new AtomicInteger();
    private final AtomicInteger numRequestedContainers = new AtomicInteger();
    private final AtomicInteger numCompletedContainers = new AtomicInteger();
    private final AtomicInteger numFailedContainers = new AtomicInteger();

    private volatile boolean isFinished = false;

    public static void main(String[] args) {
        boolean result = false;
        try {
            ApplicationMaster appMaster = new ApplicationMaster();
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            logger.fatal("Error running AM", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            logger.info("AM completed successfully");
            System.exit(0);
        } else {
            logger.info("AM failed");
            System.exit(2);
        }
    }

    public ApplicationMaster() {
        this.containerMainClass = "hivemall.mix.server.MixServer";
        this.conf = new YarnConfiguration();
        this.opts = new Options();
        opts.addOption("num_containers", true, "# of containers for MIX servers");
        opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run a MIX server");
        opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run a MIX server");
        opts.addOption("priority", true, "Application Priority [Default: 0]");
        opts.addOption("num_retries", true, "# of retries for failed containers [Default: 32]");
        opts.addOption("help", false, "Print usage");
    }

    // Helper function to print out usage
    private void printUsage() {
        new HelpFormatter().printHelp("ApplicatonMaster", opts);
    }

    public boolean init(String[] args) throws ParseException, IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "No args specified for MixServerRunner to initialize");
        }

        CommandLine cliParser = new GnuParser().parse(opts, args);
        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        // Get variables from envs
        appAttemptID = ConverterUtils.toContainerId(getEnv(Environment.CONTAINER_ID.name()))
                .getApplicationAttemptId();
        sharedDir = getEnv(MixEnv.MIXSERVER_RESOURCE_LOCATION);
        mixServJar = getEnv(MixEnv.MIXSERVER_CONTAINER_APP);

        // Get variables from arguments
        containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        numRetryForFailedContainers = Integer.parseInt(cliParser.getOptionValue("num_retries", "32"));
        if (numContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run distributed shell with no containers");
        }

        try {
            Log4jPropertyHelper.updateLog4jConfiguration(
                    ApplicationMaster.class, "log4j.properties");
        } catch (Exception e) {
            logger.warn("Can not set up custom log4j properties. " + e);
        }

        // Build an executable command for containers
        cmdInfo.init();

        logger.info("Application master for "
                + "appId:" + appAttemptID.getApplicationId().getId()
                + ", clusterTimestamp:" + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId:" + appAttemptID.getAttemptId()
                + ", containerVCores:" + containerVCores
                + ", containerMemory:" + containerMemory
                + ", numContainers:" + numContainers
                + ", requestPriority:" + requestPriority);

        return true;
    }

    private String getEnv(String key) {
        final String value = System.getenv(key);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(key + "not set in the environment");
        }
        return value;
    }

    public void run() throws YarnException, IOException, InterruptedException {
        // AM <--> RM
        amRMClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
        amRMClientAsync.init(conf);
        amRMClientAsync.start();

        // AM <--> NM
        containerListener = new NMCallbackHandler(this);
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // Register self with ResourceManager to start
        // heartbeating to the RM.
        RegisterApplicationMasterResponse response = amRMClientAsync
            .registerApplicationMaster("", -1, "");

        // A resource ask cannot exceed the max
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        if (containerVCores > maxVCores) {
            logger.warn("cores:" + containerVCores + " requested, but only cores:"
                    + maxVCores + " available.");
            containerVCores = maxVCores;
        }
        int maxMem = response.getMaximumResourceCapability().getMemory();
        if (containerMemory > maxMem) {
            logger.warn("mem:" + containerMemory + " requested, but only mem:"
                    + maxMem + " available.");
            containerMemory = maxMem;
        }

        // Accept heartbeats from launched MIX servers
        startNettyServer(new HeartbeatInitializer(new HeartbeatReceiver(activeMixServers)),
                MixEnv.REPORT_RECEIVER_PORT);

        // Accept resource requests from clients
        startNettyServer(new MixServerRequestInitializer(
                    new MixServerRequestReceiver(activeMixServers)),
                MixEnv.RESOURCE_REQUEST_PORT);

        // Start scheduled threads to check if MIX servers keep alive
        monitorContainerExecutor.scheduleAtFixedRate(
                new MonitorContainerRunnable(amRMClientAsync, activeMixServers, allocContainers),
                MixEnv.MIXSERVER_HEARTBEAT_INTERVAL + 30L,
                MixEnv.MIXSERVER_HEARTBEAT_INTERVAL,
                TimeUnit.SECONDS);

        for (int i = 0; i < numContainers; i++) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClientAsync.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numContainers);
    }

    private void startNettyServer(ChannelInitializer initializer, int port)
            throws InterruptedException {
        final EventLoopGroup boss = new NioEventLoopGroup(1);
        final EventLoopGroup workers = new NioEventLoopGroup(1);
        nettyWorkers.add(boss);
        nettyWorkers.add(workers);
        ServerBootstrap b = new ServerBootstrap();
        b.option(ChannelOption.SO_KEEPALIVE, true);
        b.group(boss, workers);
        b.channel(NioServerSocketChannel.class);
        b.handler(new LoggingHandler(LogLevel.INFO));
        b.childHandler(initializer);
        // Bind and start to accept incoming connections
        b.bind(port).sync();
    }

    @ThreadSafe
    public final class MonitorContainerRunnable implements Runnable {

        private AMRMClientAsync amRMClientAsync;
        private final ConcurrentMap<ContainerId, TimestampedValue<NodeId>> activeMixServers;
        private final ConcurrentMap<ContainerId, Container> allocContainers;

        public MonitorContainerRunnable(
                AMRMClientAsync amRMClientAsync,
                ConcurrentMap<ContainerId, TimestampedValue<NodeId>> activeMixServers,
                ConcurrentMap<ContainerId, Container> allocContainers) {
            this.amRMClientAsync = amRMClientAsync;
            this.allocContainers = allocContainers;
            this.activeMixServers = activeMixServers;
        }

        @Override
        public void run() {
            final Set<Entry<ContainerId, TimestampedValue<NodeId>>> set =
                    activeMixServers.entrySet();
            final Iterator<Entry<ContainerId, TimestampedValue<NodeId>>> itor = set.iterator();
            while (itor.hasNext()) {
                Entry<ContainerId, TimestampedValue<NodeId>> e = itor.next();
                TimestampedValue<NodeId> value = e.getValue();
                long elapsedTime = System.currentTimeMillis() - value.getTimestamp();
                // Wait at most two-times intervals for heartbeats
                if (elapsedTime > MixEnv.MIXSERVER_HEARTBEAT_INTERVAL * 2) {
                    // If expired, restart the MIX server
                    ContainerId id = e.getKey();
                    NodeId node = value.getValue();
                    Container container = allocContainers.get(id);
                    if (container != null) {
                        // TODO: Restart the failed MIX server.
                        amRMClientAsync.releaseAssignedContainer(id);
                        itor.remove();
                    } else {
                        logger.warn(node + " failed though, " + id
                                + " already has been removed from assigned containers");
                    }
                }
            }
        }
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        private int retryRequest = 0;

        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            logger.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                final ContainerId containerId = containerStatus.getContainerId();

                logger.info(appAttemptID + " got container status for "
                        + "containerID:" + containerId
                        + ", state:" + containerStatus.getState()
                        + ", exitStatus:" + containerStatus.getExitStatus()
                        + ", diagnostics:" + containerStatus.getDiagnostics());


                // Non complete containers should not be here
                assert containerStatus.getState() == ContainerState.COMPLETE;

                // Ignore containers we know nothing about - probably
                // from a previous attempt.
                if (!allocContainers.containsKey(containerId)) {
                    logger.warn("Ignoring completed status of " + containerId
                            + "; unknown container (probably launched by previous attempt)");
                    continue;
                }

                // Unregister the container
                allocContainers.remove(containerId);
                activeMixServers.remove(containerId);

                // Increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (exitStatus != 0) {
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {
                    numCompletedContainers.incrementAndGet();
                }
            }

            // Ask for more containers if any failed
            int askCount = numContainers - numRequestedContainers.get();
            if (retryRequest++ < numRetryForFailedContainers && askCount > 0) {
                logger.info("Retry " + askCount + " requests for failed containers");
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    amRMClientAsync.addContainerRequest(containerAsk);
                }
                numRequestedContainers.addAndGet(askCount);
            }

            if (numCompletedContainers.get() == numContainers) {
                isFinished = true;
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            logger.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container container: allocatedContainers) {
                logger.info("Launching a MIX server on a new container: "
                        + "containerId=" + container.getId()
                        + ", containerNode=" + container.getNodeId().getHost()
                            + ":" + container.getNodeId().getPort()
                        + ", containerNodeURI=" + container.getNodeHttpAddress()
                        + ", containerResourceMemory=" + container.getResource().getMemory()
                        + ", containerResourceVirtualCores="
                            + container.getResource().getVirtualCores());

                allocContainers.put(container.getId(), container);

                // Launch and start the container on a separate thread to keep
                // the main thread unblocked as all containers
                // may not be allocated at one go.
                containerExecutor.submit(
                        new LaunchContainerRunnable(container, cmdInfo));
            }
        }

        @Override
        public void onShutdownRequest() {
            isFinished = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> list) {}

        @Override
        public float getProgress() {
            // Set progress to deliver to RM on next heartbeat
            return (float) numCompletedContainers.get() / numContainers;
        }

        @Override
        public void onError(Throwable throwable) {
            isFinished = true;
            amRMClientAsync.stop();
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private final ApplicationMaster appMaster;

        public NMCallbackHandler(ApplicationMaster appMaster) {
            this.appMaster = appMaster;
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            logger.info("Succeeded to start Container " + containerId);
            final Container container =
                    appMaster.allocContainers.get(containerId);
            if (container != null) {
                appMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
                // Create an invalid entry for the MIX server that is not launched yet and
                // the first heartbeat message makes this entry valid
                // in HeartbeatHandler#channelRead0.
                final NodeId node = NodeId.newInstance(container.getNodeId().getHost(), -1);
                activeMixServers.put(containerId,
                        new TimestampedValue<NodeId>(node));
            } else {
                // Ignore containers we know nothing about - probably
                // from a previous attempt.
                logger.info("Ignoring completed status of " + containerId
                        + "; unknown container (probably launched by previous attempt)");
            }
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus status) {
            logger.info("Container Status: id=" + containerId + ", status=" + status);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            logger.info("Succeeded to stop Container " + containerId);
            appMaster.allocContainers.remove(containerId);
            appMaster.activeMixServers.remove(containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to start Container " + containerId);
            appMaster.allocContainers.remove(containerId);
            appMaster.activeMixServers.remove(containerId);
            appMaster.numCompletedContainers.incrementAndGet();
            appMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to stop Container " + containerId);
            appMaster.allocContainers.remove(containerId);
            appMaster.activeMixServers.remove(containerId);
        }
    }

    private boolean finish() throws InterruptedException {
        while (!isFinished && (numCompletedContainers.get() != numContainers)) {
            Thread.sleep(60 * 1000L);
        }

        // First, shutdown the executor for launchers
        containerExecutor.shutdown();

        // When the application completes, it should stop all
        // running containers.
        nmClientAsync.stop();

        // Stop all the netty workers
        for (EventLoopGroup worker : nettyWorkers) {
            worker.shutdownGracefully();
        }

        // When the application completes, it should send a finish
        // application signal to the RM.
        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics: "
                    + "total=" + numContainers
                    + ", completed=" + numCompletedContainers.get()
                    + ", allocated=" + numAllocatedContainers.get()
                    + ", failed=" + numFailedContainers.get();
            logger.info(appMessage);
            success = false;
        }

        try {
            amRMClientAsync.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (Exception e) {
            logger.error("Failed to unregister application", e);
        }

        amRMClientAsync.stop();

        return success;
    }

    private ContainerRequest setupContainerAskForRM() {
        Priority pri = Priority.newInstance(requestPriority);
        Resource capability = Resource.newInstance(containerMemory, containerVCores);
        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        logger.info("Requested container ask: " + request.toString());
        return request;
    }

    private class ContainerLaunchInfo {

        private Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        private List<String> cmd = null;
        private boolean isInitialized = false;

        public void init() {
            // If already initialized, return
            if (isInitialized)
                return;

            // Set local resources (e.g., local files or archives)
            // for the allocated container.
            try {
                final FileSystem fs = FileSystem.get(conf);
                final Path mixServJarDst = new Path(sharedDir, mixServJar);
                localResources.put(mixServJarDst.getName(),
                        YarnUtils.createLocalResource(fs, mixServJarDst));
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Set arguments
            List<String> vargs = new ArrayList<String>();

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Create a command executed in NM
            final WorkerCommandBuilder cmdBuilder = new WorkerCommandBuilder(
                    containerMainClass, YarnUtils.getClassPaths(""), containerMemory, vargs, null);

            // Set a yarn-specific java home
            cmdBuilder.setJavaHome(Environment.JAVA_HOME.$$());

            logger.info("Build an executable command for containers: " + cmdBuilder);

            try {
                this.cmd = cmdBuilder.buildCommand();
                isInitialized = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public ContainerLaunchContext createContext() {
            assert isInitialized;
            return ContainerLaunchContext.newInstance(localResources, null, cmd, null, null, null);
        }
    }

    // Thread to launch the container that will execute a MIX server
    private class LaunchContainerRunnable implements Runnable {

        private final Container container;
        private final ContainerLaunchInfo cmdInfo;

        public LaunchContainerRunnable(Container container, ContainerLaunchInfo cmdInfo) {
            this.container = container;
            this.cmdInfo = cmdInfo;
        }

        @Override
        public void run() {
            nmClientAsync.startContainerAsync(container, cmdInfo.createContext());
        }
    }
}
