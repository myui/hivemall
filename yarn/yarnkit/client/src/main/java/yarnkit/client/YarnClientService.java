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
package yarnkit.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import yarnkit.YarnkitException;
import yarnkit.container.ContainerLaunchContextFactory;
import yarnkit.container.ContainerLaunchParameters;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 * A basic implementation of a YARN Client service.
 */
public final class YarnClientService extends AbstractScheduledService {
    private static final Log LOG = LogFactory.getLog(YarnClientService.class);
    private static final Set<YarnApplicationState> DONE = EnumSet.of(YarnApplicationState.FAILED,
        YarnApplicationState.FINISHED, YarnApplicationState.KILLED);

    @Nonnull
    private final YarnClientParameters parameters;
    @Nonnull
    private final YarnConfiguration conf;
    @Nonnull
    private final Stopwatch stopwatch;

    private YarnClient yarnClient;
    private ApplicationId applicationId;
    private ApplicationReport finalReport;
    private boolean timeout = false;

    public YarnClientService(@Nonnull YarnClientParameters params) {
        this(params, params.getConfiguration(), new Stopwatch());
    }

    public YarnClientService(@Nonnull YarnClientParameters parameters, @Nonnull Configuration conf,
            @Nonnull Stopwatch stopwatch) {
        this.parameters = Preconditions.checkNotNull(parameters);
        this.conf = new YarnConfiguration(conf);
        this.stopwatch = stopwatch;
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS); // TODO REVISEME
    }

    @Override
    protected void startUp() throws Exception {
        // Make a connection to ResourceManager
        this.yarnClient = connect();

        // Submit a YARN application
        ApplicationSubmissionContext appContext = makeApplicationContext(yarnClient);
        this.applicationId = appContext.getApplicationId();
        submitApplication(appContext);

        // Make sure we stop the application in the case that it isn't done already.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (YarnClientService.this.isRunning()) {
                    YarnClientService.this.stop();
                }
            }
        });
        stopwatch.start();
    }

    @Nonnull
    private YarnClient connect() {
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
        return client;
    }

    @Nonnull
    private ApplicationSubmissionContext makeApplicationContext(@Nonnull YarnClient yarnClient)
            throws YarnkitException {
        final YarnClientApplication clientApp;
        try {
            clientApp = yarnClient.createApplication();
        } catch (Exception e) {
            throw new YarnkitException("Failed to initialize YarnClient", e);
        }
        GetNewApplicationResponse appResp = clientApp.getNewApplicationResponse();

        ApplicationSubmissionContext appContext = clientApp.getApplicationSubmissionContext();
        appContext.setApplicationName(parameters.getApplicationName());
        appContext.setQueue(parameters.getQueue());

        final ByteBuffer serializedTokens;
        try {
            serializedTokens = getSecurityToken(conf);
        } catch (IOException e) {
            throw new YarnkitException("Failed to get a security token", e);
        }
        ContainerLaunchContextFactory clcFactory = new ContainerLaunchContextFactory(
            appResp.getMaximumResourceCapability(), serializedTokens);

        ApplicationId applicationId = appContext.getApplicationId();
        ContainerLaunchParameters appMasterParams = parameters.getApplicationMasterParameters(applicationId);
        appContext.setResource(clcFactory.createResource(appMasterParams));
        appContext.setPriority(clcFactory.createPriority(appMasterParams));
        ContainerLaunchContext clc = clcFactory.create(appMasterParams);
        appContext.setAMContainerSpec(clc);

        return appContext;
    }

    @Nullable
    private static ByteBuffer getSecurityToken(@Nonnull Configuration conf) throws IOException {
        if (!UserGroupInformation.isSecurityEnabled()) {
            return null;
        }
        FileSystem fs = FileSystem.get(conf);
        Credentials credentials = new Credentials();
        String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
        if (tokenRenewer == null || tokenRenewer.length() == 0) {
            throw new IOException(
                "Can't get Master Kerberos principal for the RM to use as renewer");
        }
        // For now, only getting tokens for the default file-system.
        final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
        if (tokens != null) {
            for (Token<?> token : tokens) {
                LOG.info("Got delegation token for " + fs.getUri() + ": " + token);
            }
        }
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    private void submitApplication(@Nonnull ApplicationSubmissionContext appContext) {
        LOG.info("Submitting an application to the ApplicationManager");
        try {
            yarnClient.submitApplication(appContext);
        } catch (YarnException e) {
            LOG.error("Exception thrown submitting application", e);
            stop();
        } catch (IOException e) {
            LOG.error("IOException thrown submitting application", e);
            stop();
        }
    }

    @Override
    protected void shutDown() {
        if (finalReport != null) {
            YarnApplicationState state = finalReport.getYarnApplicationState();
            FinalApplicationStatus status = finalReport.getFinalApplicationStatus();
            String diagnostics = finalReport.getDiagnostics();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == status) {
                    LOG.info("Application completed successfully.");
                } else {
                    LOG.info("Application finished unsuccessfully." + " State = "
                            + state.toString() + ", FinalStatus = " + status.toString());
                }
            } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not complete successfully." + " State = "
                        + state.toString() + ", FinalStatus = " + status.toString());
                if (diagnostics != null) {
                    LOG.info("Diagnostics = " + diagnostics);
                }
            }
        } else {
            // Otherwise, we need to kill the application, if it was created.
            if (applicationId != null) {
                LOG.info("Killing application id = " + applicationId);
                try {
                    yarnClient.killApplication(applicationId);
                } catch (YarnException e) {
                    LOG.error("Exception thrown killing application", e);
                } catch (IOException e) {
                    LOG.error("IOException thrown killing application", e);
                }
                LOG.info("Application was killed.");
            }
        }
    }

    @Override
    protected void runOneIteration() throws Exception {
        if (isApplicationFinished()) {
            LOG.info("Nothing to do, application is finished");
            return;
        }

        ApplicationReport report = getApplicationReport();
        if (report == null) {
            LOG.error("No application report received");
        } else if (DONE.contains(report.getYarnApplicationState())
                || report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
            finalReport = report;
            stop();
        }

        // Ensure that we haven't been running for all that long.
        if (parameters.getClientTimeoutMillis() > 0
                && stopwatch.elapsedMillis() > parameters.getClientTimeoutMillis()) {
            LOG.warn("Stopping application due to timeout.");
            timeout = true;
            stop();
        }
    }

    private boolean isApplicationFinished() {
        return timeout || finalReport != null;
    }

    @Nullable
    public ApplicationReport getFinalReport() {
        if (!timeout && finalReport == null) {
            this.finalReport = getApplicationReport();
        }
        return finalReport;
    }

    @Nullable
    public ApplicationReport getApplicationReport() {
        try {
            return yarnClient.getApplicationReport(applicationId);
        } catch (YarnException ye) {
            LOG.error("Exception occurred requesting application report", ye);
            return null;
        } catch (IOException ioe) {
            LOG.error("IOException occurred requesting application report", ioe);
            return null;
        }
    }

}
