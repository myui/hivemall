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

import static yarnkit.config.YarnkitFields.PATH_APP_RESOURCE_MAPPING;
import static yarnkit.config.YarnkitFields.TAG_FILE;
import static yarnkit.config.YarnkitFields.TAG_NAME;
import static yarnkit.config.YarnkitFields.TAG_VISIBILITY;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

import yarnkit.config.YarnkitConfig;
import yarnkit.utils.YarnUtils;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

public final class HoconConfig implements YarnkitConfig {
    private static final Log LOG = LogFactory.getLog(HoconConfig.class);

    @Nonnull
    private final Map<String, String> env;
    @Nonnull
    private final Config rootConf;
    @Nonnull
    private final Config conf;

    public HoconConfig(@Nonnull Config rootConf) {
        this(rootConf, rootConf);
    }

    private HoconConfig(@Nonnull Config rootConf, @Nonnull Config conf) {
        this.env = new HashMap<String, String>();
        this.rootConf = rootConf;
        this.conf = conf;
    }

    private HoconConfig(@Nonnull Map<String, String> env, @Nonnull Config rootConf) {
        this(env, rootConf, rootConf);
    }

    private HoconConfig(@Nonnull Map<String, String> env, @Nonnull Config rootConf,
            @Nonnull Config conf) {
        this.env = env;
        this.rootConf = rootConf;
        this.conf = conf;
    }

    @Override
    public void setEnv(String key, String value) {
        env.put(key, value);
    }

    @Override
    public Map<String, String> getEnv() {
        return env;
    }

    @Override
    public YarnkitConfig getRoot() {
        return new HoconConfig(env, rootConf);
    }

    @Override
    public File getConfigFile(@Nonnull Map<String, LocalResource> mapping) throws IOException {
        final String serialized;
        if (mapping.isEmpty()) {
            serialized = conf.root().render(ConfigRenderOptions.concise());
        } else {
            Map<String, String> props = YarnUtils.convertResourceMapToProperties(mapping);
            ConfigObject appended = ConfigValueFactory.fromMap(props);
            ConfigObject merged = conf.root().withValue(PATH_APP_RESOURCE_MAPPING, appended);
            serialized = merged.render(ConfigRenderOptions.concise());
        }
        LOG.info("Serialized application.conf: \n" + serialized);

        File file = File.createTempFile("application", ".conf");
        Files.write(serialized, file, Charsets.UTF_8);
        return file;
    }

    @Override
    public YarnkitConfig getConfig(@Nonnull String path) {
        if (!conf.hasPath(path)) {
            throw new IllegalArgumentException(path + "does not found in " + conf);
        }
        Config newConf = conf.getConfig(path);
        return new HoconConfig(env, rootConf, newConf);
    }

    @Override
    public boolean hasPath(@Nonnull String path) {
        return conf.hasPath(path);
    }

    @Override
    public boolean getBoolean(@Nonnull String path) {
        return conf.getBoolean(path);
    }

    @Override
    public String getString(@Nonnull String path) {
        return conf.getString(path);
    }

    @Override
    public String getString(@Nonnull String path, @Nullable String defaultValue) {
        String value = conf.getString(path);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    @Override
    public int getInt(@Nonnull String path) {
        return conf.getInt(path);
    }

    @Override
    public int getInt(@Nonnull String path, int defaultValue) {
        try {
            return conf.getInt(path);
        } catch (ConfigException e) {
            return defaultValue;
        }
    }

    @Override
    public long getLong(@Nonnull String path) {
        return conf.getLong(path);
    }

    @Override
    public long getLong(@Nonnull String path, long defaultValue) {
        try {
            return conf.getLong(path);
        } catch (ConfigException e) {
            return defaultValue;
        }
    }

    @Override
    public List<String> getStringList(@Nonnull String path) {
        if (!conf.hasPath(path)) {
            return Collections.emptyList();
        }
        return conf.getStringList(path);
    }

    @Override
    public Map<String, String> getProperties(@Nonnull String path) {
        if (!conf.hasPath(path)) {
            return Collections.emptyMap();
        }

        final Map<String, String> map = Maps.newHashMap();
        for (ConfigObject elem : conf.getObjectList(path)) {
            for (Map.Entry<String, ConfigValue> e : elem.entrySet()) {
                String key = e.getKey();
                String value = e.getValue().unwrapped().toString();
                map.put(key, value);
            }
        }
        return map;
    }

    @Override
    public void loadProperties(@Nonnull String path, @Nonnull Map<String, String> dst) {
        if (!conf.hasPath(path)) {
            return;
        }

        for (ConfigObject elem : conf.getObjectList(path)) {
            for (Map.Entry<String, ConfigValue> e : elem.entrySet()) {
                String key = e.getKey();
                String value = e.getValue().unwrapped().toString();
                dst.put(key, value);
            }
        }
    }

    @Override
    public void setupLocalResources(@Nonnull FileSystem fs, @Nonnull Path appDir,
            @Nonnull String path, @Nonnull Map<String, LocalResource> resourceMap)
            throws IOException {
        if (!conf.hasPath(path)) {
            return;
        }

        List<? extends Config> list = conf.getConfigList(path);
        if (list.isEmpty()) {
            return;
        }

        for (Config cfg : list) {
            String name = cfg.getString(TAG_NAME);
            String file = cfg.getString(TAG_FILE);

            LocalResource resource = YarnUtils.mapLocalResourceToHDFS(fs, file, appDir, name,
                resourceMap);
            if (cfg.hasPath(TAG_VISIBILITY)) {
                String visibility = cfg.getString(TAG_VISIBILITY).toUpperCase();
                resource.setVisibility(LocalResourceVisibility.valueOf(visibility));
            }
        }
    }

}
