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
package hivemall.xgboost;

import java.io.*;
import java.lang.reflect.Field;
import java.util.UUID;
import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class NativeLibLoader {
    private static final Log logger = LogFactory.getLog(NativeLibLoader.class);

    private static final String keyUserDefinedLib = "hivemall.xgboost.lib";
    private static final String libPath = "/lib/";

    private static boolean initialized = false;

    // Try to load a native library if it exists
    public static synchronized void initXGBoost() {
        if(!initialized) {
            // Since a user-defined native library has a top priority,
            // we first check if it is defined or not.
            final String userDefinedLib = System.getProperty(keyUserDefinedLib);
            if(userDefinedLib == null) {
                tryLoadNativeLibFromResource("xgboost4j");
            } else {
                tryLoadNativeLib(userDefinedLib);
            }
            initialized = true;
        }
    }

    private static boolean hasResource(String path) {
        return NativeLibLoader.class.getResource(path) != null;
    }

    private static String getOSName() {
        return System.getProperty("os.name");
    }

    private static void tryLoadNativeLibFromResource(final String libName) {
        // Resolve the library file name with a suffix (e.g., dll, .so, etc.)
        String resolvedLibName = System.mapLibraryName(libName);

        if(!hasResource(libPath + resolvedLibName)) {
            if(!getOSName().equals("Mac")) {
                return;
            }
            // Fix for openjdk7 for Mac
            // A detail of this workaround can be found in https://github.com/xerial/snappy-java/issues/6
            resolvedLibName = "lib" + libName + ".jnilib";
            if(hasResource(libPath + resolvedLibName)) {
                return;
            }
        }
        try {
            final File tempFile = createTempFileFromResource(
                    resolvedLibName, NativeLibLoader.class.getResourceAsStream(libPath + resolvedLibName));
            logger.info("Copyed the native library in JAR as " + tempFile.getAbsolutePath());
            addLibraryPath(tempFile.getParent());
        } catch (Exception e) {
            // Simply ignore it here
            logger.info(e.getMessage());
        }
    }

    private static void tryLoadNativeLib(final String userDefinedLib) {
        final File userDefinedLibFile = new File(userDefinedLib);
        if(!userDefinedLibFile.exists()) {
            logger.warn(userDefinedLib + " not found");
        } else {
            try {
                final File tempFile = createTempFileFromResource(
                        userDefinedLibFile.getName(),
                        new FileInputStream(userDefinedLibFile.getAbsolutePath())
                );
                logger.info("Copyed the user-defined native library as " + tempFile.getAbsolutePath());
                addLibraryPath(tempFile.getParent());
            } catch (Exception e) {
                // Simply ignore it here
                logger.warn(e.getMessage());
            }
        }
    }

    private static String getPreffix(@Nonnull String fileName) {
        int point = fileName.lastIndexOf(".");
        if(point != -1) {
            return fileName.substring(0, point);
        }
        return fileName;
    }

    /**
     * Create a temp file that copies the resource from current JAR archive.
     *
     * @param libName Library name with a suffix
     * @param is Input stream to the native library
     * @return The created temp file
     * @throws IOException
     * @throws IllegalArgumentException
     */
    static File createTempFileFromResource(String libName, InputStream is)
            throws IOException, IllegalArgumentException {
        // Create a temporary folder with a random number for the native lib
        final String uuid = UUID.randomUUID().toString();
        final File tempFolder = new File(
                System.getProperty("java.io.tmpdir"),
                String.format("%s-%s", getPreffix(libName), uuid)
            );
        if(!tempFolder.exists()) {
            boolean created = tempFolder.mkdirs();
            if(!created) {
                throw new IOException("Failed to create a temporary folder for the native lib");
            }
        }

        // Prepare buffer for data copying
        byte[] buffer = new byte[8192];
        int readBytes;

        // Open output stream and copy the native library into the temporary one
        File extractedLibFile = new File(tempFolder.getAbsolutePath(), libName);
        final OutputStream os = new FileOutputStream(extractedLibFile);
        try {
            while((readBytes = is.read(buffer)) != -1) {
                os.write(buffer, 0, readBytes);
            }
        } finally {
            // If read/write fails, close streams safely before throwing an exception
            os.close();
            is.close();
        }
        return extractedLibFile;
    }

    /**
     * Add libPath to java.library.path, then native library in libPath would be load properly.
     *
     * @param libPath library path
     * @throws IOException exception
     */
    private static void addLibraryPath(String libPath) throws IOException {
        try {
            final Field field = ClassLoader.class.getDeclaredField("usr_paths");
            field.setAccessible(true);
            final String[] paths = (String[]) field.get(null);
            for (String path : paths) {
                if(libPath.equals(path)) {
                    return;
                }
            }
            final String[] tmp = new String[paths.length + 1];
            System.arraycopy(paths, 0, tmp, 0, paths.length);
            tmp[paths.length] = libPath;
            field.set(null, tmp);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage());
            throw new IOException("Failed to get permissions to set library path");
        } catch (NoSuchFieldException e) {
            logger.error(e.getMessage());
            throw new IOException("Failed to get field handle to set library path");
        }
    }

}
