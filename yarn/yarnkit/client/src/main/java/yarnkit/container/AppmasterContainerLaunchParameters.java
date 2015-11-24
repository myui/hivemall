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

import static yarnkit.config.YarnkitFields.APP_CONFIG_FILENAME;
import static yarnkit.config.YarnkitFields.KEY_APP_CONFIG_FILENAME;
import static yarnkit.config.YarnkitFields.OPT_APPMASTER_JAR;
import static yarnkit.config.YarnkitFields.PATH_APPMASTER_CONTAINER_RESOURCES;
import static yarnkit.config.YarnkitFields.PATH_APPMASTER_JAR;
import static yarnkit.config.YarnkitFields.PATH_CONTAINER_JAR;
import static yarnkit.config.YarnkitFields.PATH_CONTAINER_RESOURCES;
import static yarnkit.config.YarnkitFields.TAG_ENV;
import static yarnkit.config.YarnkitFields.YARNKIT_APPMASTER_CLASS;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;

import yarnkit.config.YarnkitConfig;
import yarnkit.utils.YarnUtils;

import com.google.common.collect.Maps;

public final class AppmasterContainerLaunchParameters extends AbstractContainerLaunchParameters {
    private static final Log LOG = LogFactory.getLog(AppmasterContainerLaunchParameters.class);

    private final ApplicationId applicationId;

    public AppmasterContainerLaunchParameters(@Nonnull ApplicationId appId,
            @Nonnull YarnkitConfig conf, @Nonnull Configuration jobConf) {
        super(conf, jobConf);
        this.applicationId = appId;
    }

    @Override
    public List<String> getCommands() {
        /* @formatter:off */
        String cmd = Environment.JAVA_HOME.$() + "/bin/java" +
                " -server -Xmx256M" +
                " -Dlog4j.configuration=yarnkit/log4j.properties" +
                " " + YARNKIT_APPMASTER_CLASS +
                " -D " + KEY_APP_CONFIG_FILENAME + "=" + APP_CONFIG_FILENAME +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" + 
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
        /* @formatter:on */
        return Collections.singletonList(cmd);
    }

    @Override
    public Map<String, String> getEnvironment() {
        Map<String, String> env = Maps.newHashMap();
        YarnUtils.setupAppMasterEnv(env, jobConf);
        config.loadProperties(TAG_ENV, env);
        return env;
    }

    @Override
    public Map<String, LocalResource> getLocalResources() throws IOException {
        Map<String, LocalResource> resourceMap = Maps.newHashMap();
        YarnkitConfig rootConfig = config.getRoot();

        FileSystem fs = FileSystem.get(jobConf);
        Path appHdfsDir = YarnUtils.createApplicationTempDir(fs, applicationId);

        // Add jar if defined
        String appmasterJarPath = null;
        if (rootConfig.hasPath(PATH_APPMASTER_JAR)) {
            appmasterJarPath = rootConfig.getString(PATH_APPMASTER_JAR);
            YarnUtils.mapLocalResourceToHDFS(fs, appmasterJarPath, appHdfsDir, resourceMap);
        } else {
            String optAppmasterJar = jobConf.get(OPT_APPMASTER_JAR);
            if (optAppmasterJar != null) {
                YarnUtils.mapLocalResourceToHDFS(fs, optAppmasterJar, appHdfsDir, resourceMap);
            }
        }
        Map<String, LocalResource> containerResourceMapping = Maps.newHashMap();
        if (rootConfig.hasPath(PATH_CONTAINER_JAR)) {
            String containerJarPath = rootConfig.getString(PATH_CONTAINER_JAR);
            if (!containerJarPath.equals(appmasterJarPath)) {
                YarnUtils.mapLocalResourceToHDFS(fs, containerJarPath, appHdfsDir, containerResourceMapping);
            }
        }

        // Add AppMaster container resources
        rootConfig.setupLocalResources(fs, appHdfsDir, PATH_APPMASTER_CONTAINER_RESOURCES,
            resourceMap);
        // Add Application container resources
        rootConfig.setupLocalResources(fs, appHdfsDir, PATH_CONTAINER_RESOURCES,
            containerResourceMapping);
        resourceMap.putAll(containerResourceMapping);

        // Add application.conf as a YARN LocalResource
        File configFile = rootConfig.getConfigFile(containerResourceMapping);
        String filePath = configFile.getAbsolutePath();
        YarnUtils.mapLocalResourceToHDFS(fs, filePath, appHdfsDir, APP_CONFIG_FILENAME, resourceMap);

        LOG.info("Put LocalResources " + resourceMap + " on " + appHdfsDir);

        return resourceMap;
    }

}
