/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package yarnkit.container;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import yarnkit.YarnkitException;
import yarnkit.appmaster.ApplicationMasterService;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * A tracker that manages Containers.
 */
public final class ContainerTracker implements NMClientAsync.CallbackHandler {
    private static final Log LOG = LogFactory.getLog(ContainerTracker.class);

    @Nonnull
    private final ApplicationMasterService appMaster;
    @Nonnull
    private final ContainerLaunchParameters parameters;

    private NMClientAsync nodeManager;
    private ContainerLaunchContextFactory clcFactory;
    private ContainerLaunchContext context;

    // Container states
    private final ConcurrentMap<ContainerId, Container> runningContainers = Maps.newConcurrentMap();
    private final int numContainers;
    private final AtomicInteger needed;
    private final AtomicInteger started = new AtomicInteger(0);
    private final AtomicInteger completed = new AtomicInteger(0);
    private final AtomicInteger failed = new AtomicInteger(0);

    public ContainerTracker(@Nonnull ApplicationMasterService appMaster,
            @Nonnull ContainerLaunchParameters parameters) {
        this.appMaster = appMaster;
        this.parameters = parameters;
        this.numContainers = parameters.getNumContainers();
        this.needed = new AtomicInteger(numContainers);
    }

    public int getNumContainers() {
        return numContainers;
    }

    public int getCompleted() {
        return completed.get();
    }

    public boolean hasRunningContainers() {
        return !runningContainers.isEmpty();
    }

    public boolean needsContainers() {
        return needed.get() > 0;
    }

    //-----------------------------------------------------------------   
    // Container launch actions

    public void init(@Nonnull ContainerLaunchContextFactory factory, @Nonnull YarnConfiguration conf)
            throws YarnkitException {
        // start NodeMaanger
        this.nodeManager = NMClientAsync.createNMClientAsync(this);
        nodeManager.init(conf);
        nodeManager.start();

        // send Container launch requests
        this.clcFactory = factory;
        this.context = factory.create(parameters);

        Resource resourceCapability = factory.createResource(parameters);
        String[] nodes = parameters.getNodes();
        String[] racks = parameters.getRacks();
        Priority priority = factory.createPriority(parameters);
        boolean relaxedLocality = parameters.getRelaxLocality();

        final AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(
            resourceCapability, nodes, racks, priority, relaxedLocality);
        for (int j = 0; j < numContainers; j++) {
            appMaster.onContainerRequest(containerRequest);
        }
    }

    public void launchContainer(@Nonnull Container container) {
        LOG.info("Launching a container '" + container.getId() + "' on node: "
                + container.getNodeId());

        Preconditions.checkNotNull(clcFactory, "ContainerLaunchContextFactory should not be null");

        needed.decrementAndGet();
        runningContainers.put(container.getId(), container);
        nodeManager.startContainerAsync(container, clcFactory.duplicate(context));
    }

    public void kill() {
        for (Container c : runningContainers.values()) {
            nodeManager.stopContainerAsync(c.getId(), c.getNodeId());
        }
    }

    //-----------------------------------------------------------------    
    //  NMClientAsync.CallbackHandler implementations

    @Override
    public void onContainerStarted(@Nonnull ContainerId containerId,
            Map<String, ByteBuffer> allServiceResponse) {
        Container container = runningContainers.get(containerId);
        if (container != null) {
            LOG.info("Container started: " + containerId);
            started.incrementAndGet();
            nodeManager.getContainerStatusAsync(containerId, container.getNodeId());
        }
    }

    @Override
    public void onStartContainerError(@Nonnull ContainerId containerId, @Nonnull Throwable throwable) {
        LOG.warn("Got an error for starting a container: " + containerId, throwable);
        runningContainers.remove(containerId);
        completed.incrementAndGet();
        failed.incrementAndGet();
    }

    @Override
    public void onContainerStopped(@Nonnull ContainerId containerId) {
        LOG.info("Container stopped: " + containerId);
        runningContainers.remove(containerId);
        completed.incrementAndGet();
    }

    @Override
    public void onStopContainerError(@Nonnull ContainerId containerId, @Nonnull Throwable throwable) {
        LOG.error("Failed to stop a container: " + containerId, throwable);
        completed.incrementAndGet();
    }

    @Override
    public void onContainerStatusReceived(@Nonnull ContainerId containerId,
            @Nonnull ContainerStatus containerStatus) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received status for a container: " + containerId + " = " + containerStatus);
        }
    }

    @Override
    public void onGetContainerStatusError(@Nonnull ContainerId containerId,
            @Nonnull Throwable throwable) {
        LOG.error("Could not get status for container: " + containerId, throwable);
    }

}
