/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
package hivemall.mix.yarn;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public final class YarnUtils {

    static String getClassPaths(String appClassPath) {
        // Create application-specific classpaths
        StringBuilder classPaths = new StringBuilder();
        YarnUtils.addClassPath(Environment.CLASSPATH.$$(), classPaths);
        YarnUtils.addClassPath("./*", classPaths);
        YarnUtils.addClassPath("./log4j.properties", classPaths);
        // YarnUtils.addClassPath(appClassPath, classPaths);
        return classPaths.toString();
    }

    static void addClassPath(String path, StringBuilder classPaths) {
        classPaths.append(path);
        classPaths.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
    }

    static void copyFromLocalFile(FileSystem fs, Path src, Path dst, Map<String, LocalResource> resourceMap)
            throws IOException {
        fs.copyFromLocalFile(src, dst);
        if(resourceMap != null) {
            resourceMap.put(dst.getName(), YarnUtils.createLocalResource(fs, dst));
        }
    }

    static LocalResource createLocalResource(FileSystem fs, Path path) throws IOException {
        LocalResource resource = Records.newRecord(LocalResource.class);
        FileStatus fileStat = fs.getFileStatus(path);
        resource.setResource(ConverterUtils.getYarnUrlFromPath(path));
        resource.setSize(fileStat.getLen());
        resource.setTimestamp(fileStat.getModificationTime());
        resource.setType(LocalResourceType.FILE);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);
        return resource;
    }

}
