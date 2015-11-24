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
package yarnkit.config;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;

public interface YarnkitConfig {

    @Nonnull
    public YarnkitConfig getRoot();

    @Nonnull
    public File getConfigFile(@Nonnull Map<String, LocalResource> mapping) throws IOException;

    @Nonnull
    public YarnkitConfig getConfig(@Nonnull String path);

    public boolean hasPath(@Nonnull String path);

    public boolean getBoolean(@Nonnull String path);

    @Nullable
    public String getString(@Nonnull String path);

    @Nullable
    public String getString(@Nonnull String path, @Nonnull String defaultValue);

    public int getInt(@Nonnull String path);

    public int getInt(@Nonnull String path, int defaultValue);

    public long getLong(@Nonnull String path);

    public long getLong(@Nonnull String path, long defaultValue);

    @Nonnull
    public List<String> getStringList(@Nonnull String path);

    @Nonnull
    public Map<String, String> getProperties(@Nonnull String path);

    @Nonnull
    public void loadProperties(@Nonnull String path, @Nonnull Map<String, String> dst);

    @Nonnull
    public void setupLocalResources(@Nonnull FileSystem fs, @Nonnull Path appDir,
            @Nonnull String path, @Nonnull Map<String, LocalResource> resourceMap)
            throws IOException;

}
