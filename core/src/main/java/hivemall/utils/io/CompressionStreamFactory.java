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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nonnull;

import org.tukaani.xz.FinishableOutputStream;
import org.tukaani.xz.FinishableWrapperOutputStream;
import org.tukaani.xz.LZMA2InputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.UnsupportedOptionsException;

public final class CompressionStreamFactory {

    private CompressionStreamFactory() {}

    public enum CompressionAlgorithm {
        deflate, lzma;
    }

    @Nonnull
    public static InputStream createInputStream(@Nonnull final InputStream in,
            @Nonnull final CompressionAlgorithm algo) {
        switch (algo) {
            case deflate: {
                return new InflaterInputStream(in);
            }
            case lzma: {
                return new LZMA2InputStream(in, LZMA2Options.DICT_SIZE_DEFAULT);
            }
            default:
                throw new UnsupportedOperationException("Unsupported compression algorithm: "
                        + algo);
        }
    }

    @Nonnull
    public static FinishableOutputStream createOutputStream(@Nonnull final OutputStream out,
            @Nonnull final CompressionAlgorithm algo) {
        switch (algo) {
            case deflate: {
                final DeflaterOutputStream deflate = new DeflaterOutputStream(out);
                return new FinishableWrapperOutputStream(deflate) {
                    @Override
                    public void finish() throws IOException {
                        deflate.finish();
                        deflate.flush();
                    }
                };
            }
            case lzma: {
                LZMA2Options options = new LZMA2Options();
                try {
                    options.setDictSize(LZMA2Options.DICT_SIZE_DEFAULT);
                } catch (UnsupportedOptionsException e) {
                    throw new IllegalStateException("LZMA2Options#setDictSize("
                            + LZMA2Options.DICT_SIZE_DEFAULT + ") failed", e);
                }
                FinishableOutputStream wrapped = new FinishableWrapperOutputStream(out);
                return options.getOutputStream(wrapped);
            }
            default:
                throw new UnsupportedOperationException("Unsupported compression algorithm: "
                        + algo);
        }
    }
}
