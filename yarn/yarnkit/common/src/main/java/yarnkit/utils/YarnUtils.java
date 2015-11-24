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
package yarnkit.utils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.base.Strings;

public final class YarnUtils {

    public static Path createApplicationTempDir(@Nonnull FileSystem fs, @Nonnull ApplicationId appId)
            throws IOException {
        Path dir = new Path(fs.getHomeDirectory(), appId.toString());
        if (!fs.exists(dir)) {
            fs.mkdirs(dir);
            fs.deleteOnExit(dir);
        }
        return dir;
    }

    public static LocalResource mapLocalResourceToHDFS(@Nonnull FileSystem fs,
            @Nonnull String srcLocal, @Nonnull Path dstHdfsDir,
            @Nonnull Map<String, LocalResource> resourceMap) throws IOException {
        Path srcLocalPath = new Path(srcLocal);
        String resourceName = srcLocalPath.getName();
        return mapLocalResourceToHDFS(fs, srcLocalPath, dstHdfsDir, resourceName, resourceMap);
    }

    public static LocalResource mapLocalResourceToHDFS(@Nonnull FileSystem fs,
            @Nonnull String srcLocal, @Nonnull Path dstHdfsDir, @Nonnull String resourceName,
            @Nonnull Map<String, LocalResource> resourceMap) throws IOException {
        Path srcLocalPath = new Path(srcLocal);
        return mapLocalResourceToHDFS(fs, srcLocalPath, dstHdfsDir, resourceName, resourceMap);
    }

    public static LocalResource mapLocalResourceToHDFS(@Nonnull FileSystem fs,
            @Nonnull Path srcLocalPath, @Nonnull Path dstHdfsDir, @Nonnull String resourceName,
            @Nonnull Map<String, LocalResource> resourceMap) throws IOException {
        if (!fs.isDirectory(dstHdfsDir)) {
            throw new IllegalStateException("Expected a directory, not a file: " + dstHdfsDir);
        }
        if (!fs.exists(dstHdfsDir)) {
            throw new IllegalStateException("HDFS directory does not exist: " + dstHdfsDir);
        }
        Path dstPath = new Path(dstHdfsDir, resourceName);

        LocalResource resource = copyToHDFS(fs, srcLocalPath, dstPath);
        resourceMap.put(resourceName, resource);
        return resource;
    }

    public static LocalResource copyToHDFS(@Nonnull FileSystem fs, @Nonnull Path srcLocalPath,
            @Nonnull Path dstHdfsPath) throws IOException {
        if (!"hdfs".equals(srcLocalPath.toUri().getScheme())) {
            fs.copyFromLocalFile(srcLocalPath, dstHdfsPath);
        }
        return createLocalResource(fs, dstHdfsPath);
    }

    @Nonnull
    public static LocalResource createLocalResource(@Nonnull FileSystem fs, @Nonnull Path hdfsPath)
            throws IOException {
        LocalResource resource = Records.newRecord(LocalResource.class);
        FileStatus fileStat = fs.getFileStatus(hdfsPath);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(hdfsPath));
        resource.setSize(fileStat.getLen());
        resource.setTimestamp(fileStat.getModificationTime());
        resource.setType(fileStat.isFile() ? LocalResourceType.FILE : LocalResourceType.ARCHIVE);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);
        return resource;
    }

    @Nonnull
    public static ByteBuffer getSecurityToken(@Nonnull final Credentials credentials)
            throws IOException {
        DataOutputBuffer buf = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(buf);
        return ByteBuffer.wrap(buf.getData());
    }

    public static void removeToken(@Nonnull final Credentials credentials,
            @Nonnull final Text tokenKindToRemove) {
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            if (token.getKind().equals(tokenKindToRemove)) {
                iter.remove();
            }
        }
    }

    public static void setupAppMasterEnv(@Nonnull final Map<String, String> env,
            @Nonnull final Configuration conf) {
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), c.trim(),
                ":");
        }
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(),
            ApplicationConstants.Environment.PWD.$() + File.separator + "*", ":");
    }

    /**
     * Returns the full path to the Jar containing the class.
     *
     * @param klass class.
     * @return path to the Jar containing the class.
     */
    @Nullable
    public static String findJar(@Nonnull String klass, @Nullable ClassLoader loader) {
        if (loader != null) {
            String class_file = klass.replaceAll("\\.", "/") + ".class";
            try {
                for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
                    URL url = itr.nextElement();
                    String path = url.getPath();
                    if (path.startsWith("file:")) {
                        path = path.substring("file:".length());
                    }
                    path = URLDecoder.decode(path, "UTF-8");
                    if ("jar".equals(url.getProtocol())) {
                        path = URLDecoder.decode(path, "UTF-8");
                        return path.replaceAll("!.*$", "");
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Nonnull
    public static Map<String, String> convertResourceMapToProperties(
            @Nonnull Map<String, LocalResource> mapping) {
        if (mapping.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, String> converted = new HashMap<String, String>(mapping.size());
        for (Map.Entry<String, LocalResource> e : mapping.entrySet()) {
            String name = e.getKey();
            LocalResource resource = e.getValue();
            org.apache.hadoop.yarn.api.records.URL url = resource.getResource();
            final Path path;
            try {
                path = ConverterUtils.getPathFromYarnURL(url);
            } catch (URISyntaxException ex) {
                throw new IllegalStateException("Failed to convert YARN URL to Path: " + url, ex);
            }
            String value = path.toString();
            converted.put(name, value);
        }
        return converted;
    }

    @Nonnull
    public static Map<String, LocalResource> convertPropertiesToResouceMap(@Nonnull FileSystem fs,
            @Nonnull Map<String, String> mapping) throws IOException {
        if (mapping.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, LocalResource> converted = new HashMap<String, LocalResource>(
            mapping.size());
        for (Map.Entry<String, String> e : mapping.entrySet()) {
            String name = e.getKey();
            String value = e.getValue();
            Path path = new Path(value);
            LocalResource resource = createLocalResource(fs, path);
            converted.put(name, resource);
        }
        return converted;
    }

    public static String getContainerExitStatusMessage(@Nonnull ContainerStatus status) {
        String containerId = status.getContainerId().toString();

        final String msg;
        final int exitStatus = status.getExitStatus();
        switch (exitStatus) {
            case ContainerExitStatus.SUCCESS: {
                msg = String.format("Container %s finished succesfully", containerId);
                break;
            }
            case ContainerExitStatus.ABORTED: {
                msg = String.format("Container %s aborted", containerId);
                break;
            }
            case ContainerExitStatus.DISKS_FAILED: {
                msg = String.format("Container %s ran out of disk", containerId);
                break;
            }
            case ContainerExitStatus.PREEMPTED: {
                msg = String.format("Container %s preempted", containerId);
                break;
            }
            case ContainerExitStatus.INVALID:
            default: {
                msg = String.format("Container %s exited with an invalid/unknown exit code: %d",
                    containerId, exitStatus);
                break;
            }
        }

        String diagnostics = status.getDiagnostics();
        if (exitStatus == ContainerExitStatus.SUCCESS || Strings.isNullOrEmpty(diagnostics)) {
            return msg;
        } else {
            return msg + "\nDiagnostics: " + diagnostics;
        }
    }
}
