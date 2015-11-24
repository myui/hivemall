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

import static yarnkit.config.YarnkitFields.PATH_APPMASTER_CONTAINER;
import static yarnkit.config.YarnkitFields.PATH_APP_CLIENT_TIMEOUT;
import static yarnkit.config.YarnkitFields.PATH_APP_CONF;
import static yarnkit.config.YarnkitFields.PATH_APP_NAME;
import static yarnkit.config.YarnkitFields.PATH_APP_QUEUE;

import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import yarnkit.config.YarnkitConfig;
import yarnkit.container.AppmasterContainerLaunchParameters;
import yarnkit.container.ContainerLaunchParameters;

public final class YarnClientParametersImpl implements YarnClientParameters {

    @Nonnull
    private final YarnkitConfig config;
    @Nonnull
    private final Configuration jobConf;

    public YarnClientParametersImpl(@Nonnull YarnkitConfig config, @Nonnull Configuration jobConf) {
        this.config = config;
        this.jobConf = initJobConf(jobConf, config);
    }

    private static Configuration initJobConf(@Nonnull Configuration jobConf,
            @Nonnull YarnkitConfig config) {
        Map<String, String> conf = config.getProperties(PATH_APP_CONF);
        for (Entry<String, String> e : conf.entrySet()) {
            jobConf.set(e.getKey(), e.getValue());
        }
        return jobConf;
    }

    @Override
    public Configuration getConfiguration() {
        return jobConf;
    }

    @Override
    public String getApplicationName() {
        return config.getString(PATH_APP_NAME);
    }

    @Override
    public String getQueue() {
        return config.getString(PATH_APP_QUEUE, "default");
    }

    @Override
    public long getClientTimeoutMillis() {
        return config.getLong(PATH_APP_CLIENT_TIMEOUT, -1L);
    }

    @Override
    public ContainerLaunchParameters getApplicationMasterParameters(
            @Nonnull ApplicationId applicationId) {
        YarnkitConfig amConf = config.getConfig(PATH_APPMASTER_CONTAINER);
        return new AppmasterContainerLaunchParameters(applicationId, amConf, jobConf);
    }

}
