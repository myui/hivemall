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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import yarnkit.YarnkitException;

import com.google.common.base.Preconditions;

/**
 * Factory for constructing YARN objects from {@code ContainerLaunchParameters}.
 */
public final class ContainerLaunchContextFactory {

    @Nonnull
    private final Resource resourceCapability;
    @Nullable
    private final ByteBuffer securityTokens;

    public ContainerLaunchContextFactory(@Nonnull Resource resourceCapability,
            @Nullable ByteBuffer securityTokens) {
        this.resourceCapability = resourceCapability;
        this.securityTokens = securityTokens;
    }

    @Nonnull
    public ContainerLaunchContext create(@Nonnull ContainerLaunchParameters parameters)
            throws YarnkitException {
        ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
        context.setCommands(Collections.synchronizedList(parameters.getCommands()));
        context.setEnvironment(Collections.synchronizedMap(parameters.getEnvironment()));
        try {
            context.setLocalResources(Collections.synchronizedMap(parameters.getLocalResources()));
        } catch (IOException e) {
            throw new YarnkitException("Failed to setLocalResource to ContainerLaunchContext", e);
        }
        if (securityTokens != null) {
            context.setTokens(securityTokens.duplicate());
        }
        return context;
    }

    public ContainerLaunchContext duplicate(@CheckForNull ContainerLaunchContext original) {
        Preconditions.checkNotNull(original, "ContainerLaunchContext should not be null");

        ContainerLaunchContext copy = Records.newRecord(ContainerLaunchContext.class);
        copy.setCommands(original.getCommands());
        copy.setEnvironment(original.getEnvironment());
        copy.setLocalResources(original.getLocalResources());
        ByteBuffer token = original.getTokens();
        if (token != null) {
            copy.setTokens(token.duplicate());
        }
        return copy;
    }

    @Nonnull
    public Resource createResource(@Nonnull ContainerLaunchParameters parameters) {
        return parameters.getContainerResource(resourceCapability);
    }

    @Nonnull
    public Priority createPriority(@Nonnull ContainerLaunchParameters parameters) {
        int priority = parameters.getPriority();
        Priority p = Records.newRecord(Priority.class);
        p.setPriority(priority);
        return p;
    }

    @Nullable
    public String getNodeLabelExpression(@Nonnull ContainerLaunchParameters parameters) {
        return parameters.getNodeLabelsExpression();
    }

}
