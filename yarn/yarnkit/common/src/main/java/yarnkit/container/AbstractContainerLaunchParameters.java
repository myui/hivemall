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

import static yarnkit.config.YarnkitFields.TAG_COMMAND;
import static yarnkit.config.YarnkitFields.TAG_ENV;
import static yarnkit.config.YarnkitFields.TAG_INSTANCES;
import static yarnkit.config.YarnkitFields.TAG_MEMORY;
import static yarnkit.config.YarnkitFields.TAG_NODES;
import static yarnkit.config.YarnkitFields.TAG_NODE_LABEL;
import static yarnkit.config.YarnkitFields.TAG_PRIORITY;
import static yarnkit.config.YarnkitFields.TAG_RACKS;
import static yarnkit.config.YarnkitFields.TAG_RELAX_LOCALITY;
import static yarnkit.config.YarnkitFields.TAG_VCORES;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import yarnkit.config.YarnkitConfig;
import yarnkit.utils.YarnUtils;

import com.google.common.collect.Maps;

public abstract class AbstractContainerLaunchParameters implements ContainerLaunchParameters {

    @Nonnull
    protected final YarnkitConfig config;
    @Nonnull
    protected final Configuration jobConf;

    public AbstractContainerLaunchParameters(@Nonnull YarnkitConfig conf,
            @Nonnull Configuration jobConf) {
        this.config = conf;
        this.jobConf = jobConf;
    }

    private int getPositiveInt(@Nonnull String tag) {
        int i = config.getInt(tag);
        if (i < 1) {
            throw new IllegalStateException("Value of '" + tag + "' MUST be a positive int: " + i);
        }
        return i;
    }

    @Override
    public int getNumContainers() {
        return getPositiveInt(TAG_INSTANCES);
    }

    @Override
    public int getVirtualCores() {
        return getPositiveInt(TAG_VCORES);
    }

    @Override
    public int getMemory() {
        return getPositiveInt(TAG_MEMORY);
    }

    @Override
    public Resource getContainerResource(@Nonnull Resource capability) {
        Resource resource = Records.newRecord(Resource.class);
        resource.setVirtualCores(Math.min(capability.getVirtualCores(), getVirtualCores()));
        resource.setMemory(Math.min(capability.getMemory(), getMemory()));
        return resource;
    }

    @Override
    public String[] getNodes() {
        if (!config.hasPath(TAG_NODES)) {
            return null;
        }

        List<String> list = config.getStringList(TAG_NODES);
        return list.toArray(new String[list.size()]);
    }

    @Override
    public String[] getRacks() {
        if (!config.hasPath(TAG_RACKS)) {
            return null;
        }

        List<String> list = config.getStringList(TAG_RACKS);
        return list.toArray(new String[list.size()]);
    }

    @Override
    public int getPriority() {
        int i = config.getInt(TAG_PRIORITY);
        if (i < -1) {
            throw new IllegalArgumentException("Illegal priority value: " + i);
        }
        return i;
    }

    @Override
    public boolean getRelaxLocality() {
        return config.getBoolean(TAG_RELAX_LOCALITY);
    }

    @Override
    public String getNodeLabelsExpression() {
        return config.getString(TAG_NODE_LABEL);
    }

    @Override
    public List<String> getCommands() {
        String cmd = config.getString(TAG_COMMAND);
        return Collections.singletonList(cmd);
    }

    @Override
    public Map<String, String> getEnvironment() {
        Map<String, String> env = Maps.newHashMap();
        YarnUtils.configureClasspath(env, jobConf);
        config.loadProperties(TAG_ENV, env);
        env.putAll(config.getEnv());
        return env;
    }

    @Override
    public abstract Map<String, LocalResource> getLocalResources() throws IOException;

}
