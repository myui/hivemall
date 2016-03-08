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

import static yarnkit.config.YarnkitFields.PATH_APPMASTER_ALLOWED_FAILURES;
import static yarnkit.config.YarnkitFields.PATH_APPMASTER_CLIENT_PORT;
import static yarnkit.config.YarnkitFields.PATH_APPMASTER_CONF;
import static yarnkit.config.YarnkitFields.PATH_APPMASTER_HOSTNAME;
import static yarnkit.config.YarnkitFields.PATH_APPMASTER_TRACKING_URL;
import static yarnkit.config.YarnkitFields.PATH_CONTAINER;

import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import yarnkit.config.YarnkitConfig;
import yarnkit.container.ApplicationContainerLaunchParameters;
import yarnkit.container.ContainerLaunchParameters;

public class ApplicationMasterParametersImpl implements ApplicationMasterParameters {

    @Nonnull
    protected final YarnkitConfig config;
    @Nonnull
    protected final Configuration jobConf;

    public ApplicationMasterParametersImpl(@Nonnull YarnkitConfig config,
            @Nonnull Configuration jobConf) {
        this.config = config;
        this.jobConf = initJobConf(jobConf, config);
    }

    private static Configuration initJobConf(@Nonnull Configuration jobConf,
            @Nonnull YarnkitConfig config) {
        Map<String, String> conf = config.getProperties(PATH_APPMASTER_CONF);
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
    public String getHostname() {
        return config.getString(PATH_APPMASTER_HOSTNAME, NetUtils.getHostname());
    }

    @Override
    public int getClientPort() {
        return config.getInt(PATH_APPMASTER_CLIENT_PORT, -1);
    }

    @Override
    public String getTrackingUrl() {
        return config.getString(PATH_APPMASTER_TRACKING_URL, "");
    }

    @Override
    public int getAllowedFailures() {
        return config.getInt(PATH_APPMASTER_ALLOWED_FAILURES, 1);
    }

    @Override
    public ContainerLaunchParameters getContainerLaunchParameters() {
        YarnkitConfig clpConfig = config.getConfig(PATH_CONTAINER);
        return new ApplicationContainerLaunchParameters(clpConfig, jobConf);
    }

}
