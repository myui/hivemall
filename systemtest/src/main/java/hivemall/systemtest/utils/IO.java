/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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
package hivemall.systemtest.utils;

import com.google.common.io.Resources;
import hivemall.utils.lang.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class IO {
    public static final String RD = "\t"; // row delimiter
    public static final String QD = "\n"; // query delimiter


    private IO() {}


    public static String getFromFullPath(String fullPath, Charset charset) {
        Preconditions.checkArgument(new File(fullPath).exists(), "%s is not found", fullPath);

        return new String(readAllBytes(fullPath), charset);
    }

    public static String getFromFullPath(String fullPath) {
        return getFromFullPath(fullPath, Charset.defaultCharset());
    }

    public static String getFromResourcePath(String resourcePath, Charset charset) {
        String fullPath = Resources.getResource(resourcePath).getPath();
        return getFromFullPath(fullPath, charset);
    }

    public static String getFromResourcePath(String resourcePath) {
        return getFromResourcePath(resourcePath, Charset.defaultCharset());
    }

    private static byte[] readAllBytes(String filePath) {
        File f = new File(filePath);

        int len = (int) f.length();
        byte[] buf = new byte[len];

        InputStream is = null;
        try {
            try {
                is = new FileInputStream(f);
                is.read(buf);
            } finally {
                if (is != null)
                    is.close();
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to read " + filePath + ". " + ex.getMessage());
        }

        return buf;
    }
}
