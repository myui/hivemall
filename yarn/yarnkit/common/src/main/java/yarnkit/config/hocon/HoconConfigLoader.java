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
package yarnkit.config.hocon;

import static yarnkit.config.YarnkitFields.PATH_APPLICATION;
import static yarnkit.config.YarnkitFields.PATH_APPMASTER_CONTAINER_RESOURCES;
import static yarnkit.config.YarnkitFields.PATH_CONTAINER_RESOURCES;
import static yarnkit.config.YarnkitFields.TAG_FILE;
import static yarnkit.config.YarnkitFields.TAG_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.annotation.Nonnull;

import yarnkit.config.YarnkitConfig;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;

public final class HoconConfigLoader {

    @Nonnull
    public static YarnkitConfig load(@Nonnull String configResource) {
        final Config conf;
        File file = new File(configResource);
        if (file.exists() && file.isFile()) {
            conf = ConfigFactory.parseFile(file, ConfigParseOptions.defaults()).resolve(
                ConfigResolveOptions.defaults());
        } else {
            conf = ConfigFactory.load(configResource);
        }
        validateConfig(conf);

        return new HoconConfig(conf);
    }

    @Nonnull
    public static YarnkitConfig load(@Nonnull InputStream in) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        Config conf = ConfigFactory.parseReader(reader);
        conf.resolve(ConfigResolveOptions.defaults());

        validateConfig(conf);

        return new HoconConfig(conf);
    }

    private static void validateConfig(@Nonnull Config conf) {
        Config reference = ConfigFactory.parseResources("yarnkit/reference.conf");
        conf.checkValid(reference, PATH_APPLICATION);

        for (Config cfg : conf.getConfigList(PATH_APPMASTER_CONTAINER_RESOURCES)) {
            validateResource(cfg);
        }
        for (Config cfg : conf.getConfigList(PATH_CONTAINER_RESOURCES)) {
            validateResource(cfg);
        }
    }

    private static void validateResource(@Nonnull Config conf) {
        if (!conf.hasPath(TAG_NAME)) {
            throw new IllegalArgumentException("name must be specified for resource: " + conf);
        }
        if (!conf.hasPath(TAG_FILE)) {
            throw new IllegalArgumentException("file must be specified for resource: " + conf);
        }
    }

}
