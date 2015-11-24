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

import static yarnkit.config.YarnkitFields.PATH_APP_RESOURCE_MAPPING;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.LocalResource;

import yarnkit.config.YarnkitConfig;
import yarnkit.utils.YarnUtils;

public final class ApplicationContainerLaunchParameters extends AbstractContainerLaunchParameters {

    public ApplicationContainerLaunchParameters(@Nonnull YarnkitConfig conf,
            @Nonnull Configuration jobConf) {
        super(conf, jobConf);
    }

    @Override
    public Map<String, LocalResource> getLocalResources() throws IOException {
        YarnkitConfig rootConfig = config.getRoot();

        if (!rootConfig.hasPath(PATH_APP_RESOURCE_MAPPING)) {
            return Collections.emptyMap();
        }

        Map<String, String> mapping = rootConfig.getProperties(PATH_APP_RESOURCE_MAPPING);
        if (mapping.isEmpty()) {
            return Collections.emptyMap();
        }

        FileSystem fs = FileSystem.get(jobConf);
        Map<String, LocalResource> resourceMap = YarnUtils.convertPropertiesToResouceMap(fs,
            mapping);
        return resourceMap;
    }

}
