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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

import hivemall.mix.yarn.launcher.WorkerCommandBuilder;
import hivemall.mix.yarn.network.HeartbeatHandler.HeartbeatReceiverInitializer;
import hivemall.mix.yarn.network.HeartbeatHandler.HeartbeatReceiver;
import hivemall.mix.yarn.network.MixRequestServerHandler.MixServerRequestInitializer;
import hivemall.mix.yarn.network.MixRequestServerHandler.MixRequestReceiver;
import hivemall.mix.yarn.network.NettyUtils;
import hivemall.mix.yarn.utils.TimestampedValue;
import hivemall.mix.yarn.utils.YarnUtils;

public class ApplicationMaster {
    private static final Log logger = LogFactory.getLog(ApplicationMaster.class);

    private static enum MixServEvent {
        MIXSERV_APP_ATTEMPT_START,
        MIXSERV_APP_ATTEMPT_END,
        MIXSERV_CONTAINER_START,
        MIXSERV_CONTAINER_END,
    }

    private static enum MixServEntity {
        MIXSERV_APP_ATTEMPT,
        MIXSERV_CONTAINER,
    }

    private String containerMainClass;
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

    // Parameters for containers
    private int containerMemory;
    private int containerVCores;
    private int numContainers;
    private int requestPriority;
    private int numRetryForFailedContainers;

    // In both secure and non-secure modes, this points to the job-submitter
    private UserGroupInformation appSubmitterUgi;

    private ByteBuffer allTokens;

    // Timeline client
    // TODO: Support domains and ACLs for YARN v2.6 or more
    private boolean isLogPublished;
    private TimelineClient timelineClient;

    // List of allocated containers and alive MIX servers
    private final ConcurrentMap<String, Container> allocContainers = new ConcurrentHashMap<String, Container>();
    private final ConcurrentMap<String, TimestampedValue<NodeId>> activeMixServers = new ConcurrentHashMap<String, TimestampedValue<NodeId>>();

    // Thread pool for container launchers
    private final ExecutorService containerExecutor = Executors.newFixedThreadPool(1);

    // Check if MIX servers keep alive
    private final ScheduledExecutorService monitorContainerExecutor = Executors.newScheduledThreadPool(1);

    // Group for netty workers
    private final Set<EventLoopGroup> nettyWorkers = new HashSet<EventLoopGroup>();

    // Trackers for container status
    private final AtomicInteger numAllocatedContainers = new AtomicInteger();
    private final AtomicInteger numRequestedContainers = new AtomicInteger();
    private final AtomicInteger numFailedContainers = new AtomicInteger();

    private volatile boolean isTerminated = false;

    public static void main(String[] args) {
        main(new ApplicationMaster(), args);
    }

    protected static void main(ApplicationMaster appMaster, String[] args) {
        boolean result = false;
        try {
            boolean doRun = appMaster.init(args);
            if(!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch(Throwable t) {
            logger.fatal("Error running AM", t);
            ExitUtil.terminate(1, t);
        }
        if(result) {
            logger.info("AM completed successfully");
            System.exit(0);
        } else {
            logger.info("AM failed");
            System.exit(2);
        }
    }

    public ApplicationMaster() {
        this.containerMainClass = "hivemall.mix.yarn.server.MixYarnServer";
        this.opts = new Options();
        this.conf = new YarnConfiguration();
        opts.addOption("", true, "# of containers for MIX servers");
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

    protected boolean init(String[] args) throws ParseException, IOException {
        if(args.length == 0) {
            throw new IllegalArgumentException("No args specified for MixServerRunner to initialize");
        }

        CommandLine cliParser = new GnuParser().parse(opts, args);
        if(cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        // Get variables from envs
        appAttemptID = ConverterUtils.toContainerId(getEnv(Environment.CONTAINER_ID.name())).getApplicationAttemptId();
        sharedDir = getEnv(MixYarnEnv.MIXSERVER_RESOURCE_LOCATION);
        mixServJar = getEnv(MixYarnEnv.MIXSERVER_CONTAINER_APP);

        // Get variables from arguments
        containerVCores = Integer.parseInt(cliParser.getOptionValue("container_vcores", "1"));
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
        numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        numRetryForFailedContainers = Integer.parseInt(cliParser.getOptionValue("num_retries", "32"));
        if(numContainers == 0) {
            throw new IllegalArgumentException("Cannot run distributed shell with no containers");
        }

        // Check if AM publishes logs into a timeline server
        isLogPublished = cliParser.hasOption("publish_logs");

        // Check if NMs monitor virtual memory limits
        if(conf.getBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, YarnConfiguration.DEFAULT_NM_VMEM_CHECK_ENABLED)) {
            logger.warn("Recommended: set 'yarn.nodemanager.vmem-check-enabled' at false");
        }

        logger.info("Application master for " + "appId:" + appAttemptID.getApplicationId().getId()
                + ", clusterTimestamp:" + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId:" + appAttemptID.getAttemptId() + ", containerVCores:"
                + containerVCores + ", containerMemory:" + containerMemory + ", numContainers:"
                + numContainers + ", requestPriority:" + requestPriority);

        return true;
    }

    private String getEnv(String key) {
        final String value = System.getenv(key);
        if(value == null) {
            return "";
        }
        return value;
    }

    // Visible for testing
    protected void setAmRMClient(AMRMClientAsync client) {
        this.amRMClientAsync = client;
    }

    // Visible for testing
    protected RMCallbackHandler getRMCallbackHandler() {
        return new RMCallbackHandler(Thread.currentThread());
    }

    // Visible for testing
    protected void setNumContainers(int numContainers) {
        this.numContainers = numContainers;
    }

    // Visible for testing
    protected void setNumRequestedContainers(int numRequestedContainers) {
        this.numRequestedContainers.set(numRequestedContainers);
    }

    // Visible for testing
    protected int getNumAllocatedContainers() {
        return this.numAllocatedContainers.get();
    }

    // Visible for testing
    protected int getNumFailedContainers() {
        return this.numFailedContainers.get();
    }

    protected boolean isTerminated() {
        return isTerminated;
    }

    protected void run() throws YarnException, IOException, InterruptedException {
        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        logger.info("Executing with tokens:");
        while(iter.hasNext()) {
            Token<?> token = iter.next();
            logger.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
        appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);

        // AM <--> RM
        amRMClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler(Thread.currentThread()));
        amRMClientAsync.init(conf);
        amRMClientAsync.start();

        // AM <--> NM
        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        if (isLogPublished) {
            startTimelineClient(conf);
            if (timelineClient != null) {
                publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                        MixServEvent.MIXSERV_APP_ATTEMPT_START, appSubmitterUgi);
            }
        }

        // Register self with ResourceManager to start
        // heartbeating to the RM.
        RegisterApplicationMasterResponse response = amRMClientAsync.registerApplicationMaster("", -1, "");

        // A resource ask cannot exceed the max
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        if(containerVCores > maxVCores) {
            logger.warn("cores:" + containerVCores + " requested, but only cores:" + maxVCores
                    + " available.");
            containerVCores = maxVCores;
        }
        int maxMem = response.getMaximumResourceCapability().getMemory();
        if(containerMemory > maxMem) {
            logger.warn("mem:" + containerMemory + " requested, but only mem:" + maxMem
                    + " available.");
            containerMemory = maxMem;
        }

        // Accept heartbeats from launched MIX servers
        startNettyServer(new HeartbeatReceiverInitializer(new HeartbeatReceiver(activeMixServers)), MixYarnEnv.REPORT_RECEIVER_PORT);

        // Accept resource requests from clients
        startNettyServer(new MixServerRequestInitializer(new MixRequestReceiver(activeMixServers)), MixYarnEnv.RESOURCE_REQUEST_PORT);

        // Start scheduled threads to check if MIX servers keep alive
        monitorContainerExecutor.scheduleAtFixedRate(new MonitorContainerRunnable(amRMClientAsync, activeMixServers, allocContainers), MixYarnEnv.MIXSERVER_HEARTBEAT_INTERVAL, MixYarnEnv.MIXSERVER_HEARTBEAT_INTERVAL, TimeUnit.SECONDS);

        for(int i = 0; i < numContainers; i++) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            amRMClientAsync.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numContainers);
    }

    private void startTimelineClient(final Configuration conf)
            throws YarnException, IOException, InterruptedException {
        try {
            appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
                            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
                        // Create the Timeline Client
                        timelineClient = TimelineClient.createTimelineClient();
                        timelineClient.init(conf);
                        timelineClient.start();
                    } else {
                        timelineClient = null;
                        logger.warn("Timeline service is not enabled");
                    }
                    return null;
                }
            });
        } catch (UndeclaredThrowableException e) {
            throw new YarnException(e.getCause());
        }
    }

    private static void publishApplicationAttemptEvent(
            final TimelineClient timelineClient, String appAttemptId, MixServEvent appEvent,
            UserGroupInformation ugi) {
        logger.info("Try to publish an event: appAttemptId=" + appAttemptId + ", appEvent=" + appEvent);
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(appAttemptId);
        entity.setEntityType(MixServEntity.MIXSERV_APP_ATTEMPT.toString());
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setEventType(appEvent.toString());
        event.setTimestamp(System.currentTimeMillis());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (Exception e) {
            logger.error("App Attempt "
                    + (appEvent.equals(MixServEvent.MIXSERV_APP_ATTEMPT_START) ? "start" : "end")
                    + " event could not be published for " + appAttemptId.toString(), e);
        }
    }

    private static void publishContainerStartEvent(
          final TimelineClient timelineClient, Container container, UserGroupInformation ugi) {
        logger.info("Try to publish an event: containerId="
                + container.getId() + ", appEvent=" + MixServEvent.MIXSERV_CONTAINER_START);
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getId().toString());
        entity.setEntityType(MixServEntity.MIXSERV_CONTAINER.toString());
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(MixServEvent.MIXSERV_CONTAINER_START.toString());
        event.addEventInfo("Node", container.getNodeId().toString());
        event.addEventInfo("Resources", container.getResource().toString());
        entity.addEvent(event);
        try {
            ugi.doAs(new PrivilegedExceptionAction<TimelinePutResponse>() {
                @Override
                public TimelinePutResponse run() throws Exception {
                    return timelineClient.putEntities(entity);
                }
            });
        } catch (Exception e) {
            logger.error("Container start event could not be published for " + container.getId().toString(),
                    (e instanceof UndeclaredThrowableException)? e.getCause() : e);
        }
      }

    private static void publishContainerEndEvent(
            final TimelineClient timelineClient, ContainerStatus container, UserGroupInformation ugi) {
        logger.info("Try to publish an event: containerId="
                + container.getContainerId() + ", appEvent=" + MixServEvent.MIXSERV_CONTAINER_END);
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getContainerId().toString());
        entity.setEntityType(MixServEntity.MIXSERV_CONTAINER.toString());
        entity.addPrimaryFilter("user", ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(MixServEvent.MIXSERV_CONTAINER_END.toString());
        event.addEventInfo("State", container.getState().name());
        event.addEventInfo("Exit Status", container.getExitStatus());
        entity.addEvent(event);
        try {
            timelineClient.putEntities(entity);
        } catch (Exception e) {
            logger.error("Container end event could not be published for "
                    + container.getContainerId().toString(), e);
        }
    }

    // Visible for testing
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this, activeMixServers);
    }

    private void startNettyServer(ChannelInitializer<SocketChannel> initializer, int port)
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

        private AMRMClientAsync<ContainerRequest> amRMClientAsync;
        private final ConcurrentMap<String, TimestampedValue<NodeId>> activeMixServers;
        private final ConcurrentMap<String, Container> allocContainers;

        public MonitorContainerRunnable(AMRMClientAsync<ContainerRequest> amRMClientAsync, ConcurrentMap<String, TimestampedValue<NodeId>> activeMixServers, ConcurrentMap<String, Container> allocContainers) {
            this.amRMClientAsync = amRMClientAsync;
            this.allocContainers = allocContainers;
            this.activeMixServers = activeMixServers;
        }

        @Override
        public void run() {
            final Set<Entry<String, TimestampedValue<NodeId>>> set = activeMixServers.entrySet();
            final Iterator<Entry<String, TimestampedValue<NodeId>>> itor = set.iterator();
            while(itor.hasNext()) {
                Entry<String, TimestampedValue<NodeId>> e = itor.next();
                TimestampedValue<NodeId> value = e.getValue();
                long elapsedTime = System.currentTimeMillis() - value.getTimestamp();
                // Wait at most two-times intervals for heartbeats
                logger.info("Start checking an alive set of MIX servers");
                if(elapsedTime > MixYarnEnv.MIXSERVER_HEARTBEAT_INTERVAL * 4000) {
                    // If expired, restart the MIX server
                    final String containerId = e.getKey();
                    final NodeId node = value.getValue();
                    final Container container = allocContainers.get(containerId);
                    if(container != null) {
                        // Released containers exited with ContainerExitStatus.ABORTED
                        logger.warn("Release " + container.getId() + " because heartbeats not received");
                        releaseAssignedContainer(container.getId());
                        itor.remove();
                    } else {
                        logger.warn(node + " failed though, " + containerId
                                + " already has been removed from assigned containers");
                    }
                }
            }
        }
    }

    protected void releaseAssignedContainer(ContainerId id) {
        amRMClientAsync.releaseAssignedContainer(id);
    }

    // Visible for testing
    public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        private final Thread mainThread;

        private int numRetries = 0;

        RMCallbackHandler(Thread mainThread) {
            this.mainThread = mainThread;
        }

        private void nortifyShutdown() {
            isTerminated = true;
            mainThread.interrupt();
        }

        // Visible for testing
        protected int getNumRetries() {
            return numRetries;
        }

        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            logger.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for(ContainerStatus containerStatus : completedContainers) {
                final String containerId = containerStatus.getContainerId().toString();

                logger.info(appAttemptID + " got container status for " + "containerID:"
                        + containerId + ", state:" + containerStatus.getState() + ", exitStatus:"
                        + containerStatus.getExitStatus() + ", diagnostics:"
                        + containerStatus.getDiagnostics());

                // Non complete containers should not be here
                assert containerStatus.getState() == ContainerState.COMPLETE;

                // Ignore containers we know nothing about - probably
                // from a previous attempt.
                if(!allocContainers.containsKey(containerId)) {
                    logger.warn("Ignored completed status of " + containerId
                            + "; unknown container (probably launched by previous attempt)");
                    continue;
                }

                // Unregister the container
                allocContainers.remove(containerId);
                activeMixServers.remove(containerId);

                // Adjust resource metrics
                numAllocatedContainers.decrementAndGet();
                numRequestedContainers.decrementAndGet();

                // Retry if container has some exit conditions
                int exitStatus = containerStatus.getExitStatus();
                switch(exitStatus) {
                    case ContainerExitStatus.INVALID:
                    case 143: { // Killed by yarn
                        numFailedContainers.incrementAndGet();
                    }
                    case ContainerExitStatus.SUCCESS: {
                        nortifyShutdown();
                        break;
                    }
                    case ContainerExitStatus.DISKS_FAILED:
                    case ContainerExitStatus.PREEMPTED:
                    case ContainerExitStatus.ABORTED: // Released by MonitorContainerRunnable#run()
                    default: {
                        numFailedContainers.incrementAndGet();
                        // Retry launching
                        break;
                    }
                }

                if(isLogPublished) {
                    publishContainerEndEvent(timelineClient, containerStatus, appSubmitterUgi);
                }
            }

            // Retry launching containers if not terminated
            int reAskCount = numContainers - numRequestedContainers.get();
            if(!isTerminated && reAskCount > 0) {
                if(numRetries++ < numRetryForFailedContainers) {
                    logger.warn("Retry " + reAskCount + " requests for failed containers");
                    for(int i = 0; i < reAskCount; i++) {
                        ContainerRequest containerAsk = setupContainerAskForRM();
                        amRMClientAsync.addContainerRequest(containerAsk);
                    }
                    numRequestedContainers.addAndGet(reAskCount);
                } else {
                    logger.warn("Allowable #retries exceeded; "
                            + numRequestedContainers.get() + " MIX servers alive");
                }
            }

            // Finish AM if no request
            if(numRequestedContainers.get() == 0) {
                logger.fatal("Allocation request gone for containers");
                nortifyShutdown();
            }
        }

        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            logger.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            for(Container container : allocatedContainers) {
                // TODO: Why this condition below happens?
                if(isTerminated || numAllocatedContainers.get() >= numContainers) {
                    logger.warn(container.getId() + " not accepted because of AM state"
                            + " (probably, # of allocated containers exceeded");
                    amRMClientAsync.releaseAssignedContainer(container.getId());
                    break;
                }

                logger.info("Launching a MIX server on a new container: " + "containerId="
                        + container.getId() + ", containerNode=" + container.getNodeId().getHost()
                        + ":" + container.getNodeId().getPort() + ", containerNodeURI="
                        + container.getNodeHttpAddress() + ", containerResourceMemory="
                        + container.getResource().getMemory() + ", containerResourceVirtualCores="
                        + container.getResource().getVirtualCores());

                allocContainers.put(container.getId().toString(), container);
                numAllocatedContainers.incrementAndGet();

                // Launch and start the container on a separate thread to keep
                // the main thread unblocked as all containers
                // may not be allocated at one go.
                final Runnable containerTask = createLaunchContainerThread(container, new ContainerLaunchInfo(container.getId()));
                containerExecutor.submit(containerTask);
            }
        }

        @Override
        public void onShutdownRequest() {
            logger.warn("Shutdown request received in AM");
            nortifyShutdown();
        }

        @Override
        public void onNodesUpdated(List<NodeReport> list) {}

        @Override
        public float getProgress() {
            // We assume that MIX servers has no progress,
            // so this method always returns 0.
            return 0.0f;
        }

        @Override
        public void onError(Throwable throwable) {
            logger.error("Exception thrown:" + throwable.getMessage());
            nortifyShutdown();
        }
    }

    public static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        protected final ApplicationMaster appMaster;
        protected final ConcurrentMap<String, TimestampedValue<NodeId>> activeMixServers;

        public NMCallbackHandler(ApplicationMaster appMaster, ConcurrentMap<String, TimestampedValue<NodeId>> activeMixServers) {
            this.appMaster = appMaster;
            this.activeMixServers = activeMixServers;
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            logger.info("Succeeded to start Container " + containerId);
            final Container container = appMaster.allocContainers.get(containerId.toString());
            if(container != null) {
                appMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
                // Create an invalid entry for the MIX server that is not launched yet and
                // the first heartbeat message makes this entry valid
                // in HeartbeatHandler#channelRead0.
                NodeId node = NodeId.newInstance(container.getNodeId().getHost(), -1);
                activeMixServers.put(containerId.toString(), new TimestampedValue<NodeId>(node));
            } else {
                // Ignore containers we know nothing about - probably
                // from a previous attempt.
                logger.warn("Ignored unknown container (" + containerId + "); "
                        + "probably launched by previous attempt)");
            }

            if(appMaster.timelineClient != null) {
                ApplicationMaster.publishContainerStartEvent(
                        appMaster.timelineClient, container, appMaster.appSubmitterUgi);
            }
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus status) {
            logger.info("Container Status: id=" + containerId + ", status=" + status);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            logger.info("Succeeded to stop Container " + containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to start Container " + containerId);
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {
            logger.error("Failed to stop Container " + containerId);
        }
    }

    protected boolean finish() {
        while(!isTerminated) {
            try {
                Thread.sleep(60 * 1000L);
            } catch (InterruptedException e) {
                logger.info("Sleep interrupted for shutting down AM");
                break;
            }

            if(isLogPublished) {
                publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                        MixServEvent.MIXSERV_APP_ATTEMPT_END, appSubmitterUgi);
            }

            // Show registered MIX servers if info-loglevel enabled
            if(logger.isInfoEnabled()) {
                StringBuilder sb = new StringBuilder();
                for(TimestampedValue<NodeId> node : activeMixServers.values()) {
                    if(sb.length() > 0) {
                        sb.append(",");
                    }
                    if(node.getValue().getPort() == -1) {
                        sb.append(node.getValue().getHost() + ":UNINITIALIZED");
                    } else {
                        sb.append(node);
                    }
                }
                logger.info("List of registered MIX servers: " + sb.toString());
            }
        }

        // First, shutdown the executor for launchers
        containerExecutor.shutdown();

        // When the application completes, it should stop all
        // running containers.
        nmClientAsync.stop();

        // Stop all the netty workers
        for(EventLoopGroup worker : nettyWorkers) {
            worker.shutdownGracefully();
        }

        // When the application has any failure, it logs #failures and
        // returns false; otherwise, true.
        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if(numFailedContainers.get() == 0) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Total failed count for counters:" + numFailedContainers.get();
            logger.warn(appMessage);
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

    // Visible for testing
    protected class ContainerLaunchInfo {
        private final String containerId;

        ContainerLaunchInfo(ContainerId containerId) {
            this(containerId.toString());
        }

        ContainerLaunchInfo(String containerId) {
            this.containerId = containerId;
        }

        public ContainerLaunchContext createContext() {
            // Set local resources (e.g., local files or archives)
            // for the allocated container.
            final Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            try {
                final FileSystem fs = FileSystem.get(conf);
                final Path mixServJarDst = new Path(sharedDir, mixServJar);
                localResources.put(mixServJarDst.getName(), YarnUtils.createLocalResource(fs, mixServJarDst));
            } catch(IOException e) {
                e.printStackTrace();
            }

            // Set arguments
            List<String> vargs = new ArrayList<String>();

            vargs.add("--container_id");
            vargs.add(containerId);
            vargs.add("--appmaster_host");
            vargs.add(NettyUtils.getHostAddress());
            vargs.add(String.valueOf(containerMemory));
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Workaround: Containers killed when the amounts of memory for containers and
            // MIX servers (JVMs) are the same with each other, so MIX servers
            // have smaller memory space than containers.
            int mixServMemory = (int) (containerMemory * 0.90);

            // Create a command executed in NM
            final WorkerCommandBuilder cmdBuilder = new WorkerCommandBuilder(containerMainClass, YarnUtils.getSystemClassPath(), mixServMemory, vargs, null);

            // Set a yarn-specific java home
            cmdBuilder.setJavaHome(Environment.JAVA_HOME.$$());

            logger.info("Build an executable command for containers: " + cmdBuilder);

            List<String> cmd = null;
            try {
              cmd = cmdBuilder.buildCommand();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return ContainerLaunchContext.newInstance(localResources, null, cmd, null, allTokens, null);
        }
    }

    // Visible for testing
    protected Runnable createLaunchContainerThread(Container container, ContainerLaunchInfo cmdInfo) {
        return new LaunchContainerRunnable(container, cmdInfo);
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
