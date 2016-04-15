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
package hivemall.utils.io;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import javax.annotation.Nonnull;

public final class IOUtils {
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    private IOUtils() {}

    public static void closeQuietly(final Closeable channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                ;
            }
        }
    }

    public static void closeQuietly(final Closeable... channels) {
        for (Closeable c : channels) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    ;
                }
            }
        }
    }

    /**
     * Serialize given InputStream as String.
     */
    public static String toString(@Nonnull final InputStream input) throws IOException {
        FastMultiByteArrayOutputStream output = new FastMultiByteArrayOutputStream();
        copy(input, output);
        return output.toString();
    }

    /**
     * Serialize given InputStream as String.
     */
    public static String toString(@Nonnull final InputStream input, final int bufSize)
            throws IOException {
        FastByteArrayOutputStream output = new FastByteArrayOutputStream(bufSize);
        copy(input, output);
        return output.toString();
    }

    /**
     * InputStream -> OutputStream
     */
    public static int copy(@Nonnull final InputStream input, @Nonnull final OutputStream output)
            throws IOException {
        final byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int count = 0;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    @Nonnull
    public static BufferedReader bufferedReader(@Nonnull InputStream is) {
        InputStreamReader in = new InputStreamReader(is);
        return new BufferedReader(in);
    }

}
