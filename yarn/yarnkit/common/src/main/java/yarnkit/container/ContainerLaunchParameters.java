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
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * The parameters for configuring {@code ContainerLaunchContext} that are common for server-side and
 * client-side of a YARN application.
 */
public interface ContainerLaunchParameters {

    public int getNumContainers();

    //---------------------------------------
    // capability

    public int getVirtualCores();

    public int getMemory();

    //---------------------------------------
    // container request

    @Nonnull
    public Resource getContainerResource(@Nonnull Resource capability);

    @Nullable
    public String[] getNodes();

    @Nullable
    public String[] getRacks();

    public int getPriority();

    public boolean getRelaxLocality();

    @Nullable
    public String getNodeLabelsExpression();

    //---------------------------------------
    // container launch context

    @Nonnull
    public List<String> getCommands();

    @Nonnull
    public Map<String, String> getEnvironment();

    @Nonnull
    public Map<String, LocalResource> getLocalResources() throws IOException;

}
