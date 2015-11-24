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
package yarnkit.appmaster;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

import yarnkit.container.ContainerLaunchContextFactory;
import yarnkit.container.ContainerLaunchParameters;
import yarnkit.container.ContainerTracker;
import yarnkit.utils.YarnUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 * A service that handles the server-side invocation of {@code ApplicationMaster}.
 */
public final class ApplicationMasterService extends AbstractScheduledService implements
        AMRMClientAsync.CallbackHandler {
    private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);

    @Nonnull
    private final ApplicationMasterParameters parameters;
    @Nonnull
    private final YarnConfiguration yarnConf;

    private AMRMClientAsync<ContainerRequest> resourceManager;
    private boolean hasRunningContainers = false;
    @Nullable
    private Throwable throwable;

    // application master status
    @Nullable
    private ContainerTracker tracker;
    private final AtomicInteger totalRequested = new AtomicInteger(0);
    private final AtomicInteger totalCompleted = new AtomicInteger(0);
    private final AtomicInteger totalFailures = new AtomicInteger(0);

    public ApplicationMasterService(@CheckForNull ApplicationMasterParameters parameters) {
        super();
        this.parameters = Preconditions.checkNotNull(parameters);
        this.yarnConf = new YarnConfiguration(parameters.getConfiguration());
    }

    /**
     * Returns true if there are containers still running.
     */
    public boolean hasRunningContainers() {
        return hasRunningContainers;
    }

    //-----------------------------------------------------------------
    // AMRMClientAsync.CallbackHandler implementations

    @Override
    protected void runOneIteration() throws Exception {
        if (totalFailures.get() > parameters.getAllowedFailures()) {
            stop();
        } else if (totalCompleted.get() == totalRequested.get()) {
            stop();
        }
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting Application Master");

        // create security tokens
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        ByteBuffer securityTokens = YarnUtils.getSecurityToken(credentials);

        // Create appSubmitterUgi and add original tokens to it
        String userName = System.getenv(ApplicationConstants.Environment.USER.name());
        UserGroupInformation appSubmitterUgi = UserGroupInformation.createRemoteUser(userName);
        // remove the AM->RM token so that containers cannot access it.
        YarnUtils.removeToken(credentials, AMRMTokenIdentifier.KIND_NAME);
        appSubmitterUgi.addCredentials(credentials);

        // start a resource manager (RM)
        this.resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, this);
        resourceManager.init(yarnConf);
        resourceManager.start();

        // register a application master (AM) to resource manager (RM) 
        final RegisterApplicationMasterResponse registration;
        try {
            registration = resourceManager.registerApplicationMaster(parameters.getHostname(),
                parameters.getClientPort(), parameters.getTrackingUrl());
            LOG.info("Registered Application Master: " + registration);
        } catch (Exception e) {
            LOG.error("Exception thrown registering Application Master", e);
            stop();
            return;
        }

        // assign containers
        ContainerLaunchContextFactory factory = new ContainerLaunchContextFactory(
            registration.getMaximumResourceCapability(), securityTokens);
        ContainerLaunchParameters containerLaunchParams = parameters.getContainerLaunchParameters();
        this.tracker = new ContainerTracker(this, containerLaunchParams);
        tracker.init(factory, yarnConf);
        this.hasRunningContainers = true;
    }

    @Override
    protected void shutDown() throws Exception {
        LOG.info("Stopping Containers");

        // stop containers
        tracker.kill();
        this.hasRunningContainers = false;

        // unregister application master
        final FinalApplicationStatus status;
        String message = null;
        if (state() == State.FAILED || totalFailures.get() > parameters.getAllowedFailures()) {
            status = FinalApplicationStatus.FAILED;
            if (throwable != null) {
                message = throwable.getLocalizedMessage();
            }
        } else {
            status = FinalApplicationStatus.SUCCEEDED;
        }
        LOG.info("Sending a finish request to Resource Manager: " + status);
        try {
            resourceManager.unregisterApplicationMaster(status, message, null);
        } catch (Exception e) {
            LOG.error("Error finishing Application Master", e);
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
    }

    //-----------------------------------------------------------------
    // Callback from ContainerTracker

    public void onContainerRequest(@Nonnull ContainerRequest containerRequest) {
        resourceManager.addContainerRequest(containerRequest);
        totalRequested.incrementAndGet();
    }

    //-----------------------------------------------------------------
    // AMRMClientAsync.CallbackHandler implementations

    @Override
    public void onContainersAllocated(@Nonnull List<Container> allocatedContainers) {
        LOG.info("Allocating " + allocatedContainers.size() + " container(s)");

        int assigned = 0;
        for (Container allocated : allocatedContainers) {
            if (tracker.needsContainers()) {
                tracker.launchContainer(allocated);
                ++assigned;
            }
        }

        if (assigned < allocatedContainers.size()) {
            LOG.error(String.format("Not all containers were allocated (%d out of %d)", assigned,
                allocatedContainers.size()));
            stop();
        }
    }

    @Override
    public void onContainersCompleted(@Nonnull List<ContainerStatus> containerStatuses) {
        LOG.info(containerStatuses.size() + " container(s) have completed");

        for (ContainerStatus status : containerStatuses) {
            LOG.info(YarnUtils.getContainerExitStatusMessage(status));

            int exitStatus = status.getExitStatus();
            if (exitStatus == ContainerExitStatus.SUCCESS) {
                totalCompleted.incrementAndGet();
            } else {
                if (exitStatus != ContainerExitStatus.ABORTED) {
                    totalCompleted.incrementAndGet();
                    totalFailures.incrementAndGet();
                } else {
                    // Containers killed by the framework, either due to being released by
                    // the application or being 'lost' due to node failures etc.
                }
            }
        }

    }

    @Override
    public void onShutdownRequest() {
        stop();
    }

    @Override
    public void onNodesUpdated(@Nonnull List<NodeReport> updatedNodes) {
        if (LOG.isDebugEnabled() && !updatedNodes.isEmpty()) {
            LOG.debug("Nodes updated:" + updatedNodes);
        }
    }

    @Override
    public void onError(@Nonnull Throwable e) {
        this.throwable = e;
        stop();
    }

    @Override
    public float getProgress() {
        if (tracker == null) {
            return 0.f;
        }

        int num = tracker.getCompleted();
        int den = tracker.getNumContainers();
        if (den == 0) {
            return 0.0f;
        }
        return ((float) num) / den;
    }

}
