/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.utils.io;

import hivemall.utils.lang.NumberUtils;

import java.io.File;

import javax.annotation.Nonnull;

public final class FileUtils {

    private FileUtils() {}

    public static long getFileSize(@Nonnull File file) {
        if (!file.exists()) {
            return -1L;
        }
        long size = 0;
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null && files.length > 0) {
                for (File f : files) {
                    size += getFileSize(f);
                }
            }
        } else {
            size += file.length();
        }
        return size;
    }

    public static String prettyFileSize(@Nonnull File file) {
        return NumberUtils.prettySize(getFileSize(file));
    }

}
