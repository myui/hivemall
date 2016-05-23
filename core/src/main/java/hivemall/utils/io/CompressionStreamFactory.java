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
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nonnull;

import org.tukaani.xz.FinishableWrapperOutputStream;
import org.tukaani.xz.LZMA2InputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.UnsupportedOptionsException;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

public final class CompressionStreamFactory {

    private CompressionStreamFactory() {}

    public enum CompressionAlgorithm {
        deflate, deflate_l7, xz, lzma2, lzma2_l5;
    }

    @Nonnull
    public static InputStream createInputStream(@Nonnull final InputStream in,
            @Nonnull final CompressionAlgorithm algo) {
        switch (algo) {
            case deflate:
            case deflate_l7: {
                return new InflaterInputStream(in);
            }
            case xz: {
                try {
                    return new XZInputStream(in);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to decode by XZ", e);
                }
            }
            case lzma2: { // level 6
                return new LZMA2InputStream(in, LZMA2Options.DICT_SIZE_DEFAULT);
            }
            case lzma2_l5: {// level 5
                final LZMA2Options options;
                try {
                    options = new LZMA2Options(5);
                } catch (UnsupportedOptionsException e) {
                    throw new IllegalStateException("LZMA2Option configuration failed", e);
                }
                int dictSize = options.getDictSize();
                return new LZMA2InputStream(in, dictSize);
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
                return new FinishableOutputStreamAdapter(deflate) {
                    @Override
                    public void finish() throws IOException {
                        deflate.finish();
                        deflate.flush();
                        IOUtils.finishStream(out);
                    }
                };
            }
            case deflate_l7: {
                final Deflater l7 = new Deflater(7);
                final DeflaterOutputStream deflate = new hivemall.utils.io.DeflaterOutputStream(
                    out, l7);
                return new FinishableOutputStreamAdapter(deflate) {
                    @Override
                    public void finish() throws IOException {
                        deflate.finish();
                        deflate.flush();
                        IOUtils.finishStream(out);
                    }
                };
            }
            case xz: {
                final LZMA2Options options;
                try {
                    options = new LZMA2Options(LZMA2Options.PRESET_DEFAULT);
                } catch (UnsupportedOptionsException e) {
                    throw new IllegalStateException("LZMA2Option configuration failed", e);
                }
                final XZOutputStream xz;
                try {
                    xz = new XZOutputStream(out, options);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to encode by XZ", e);
                }
                return new FinishableOutputStreamAdapter(xz) {
                    @Override
                    public void finish() throws IOException {
                        xz.finish();
                        IOUtils.finishStream(out);
                    }
                };
            }
            case lzma2: { // level 6
                final LZMA2Options options;
                try {
                    options = new LZMA2Options(LZMA2Options.PRESET_DEFAULT);
                } catch (UnsupportedOptionsException e) {
                    throw new IllegalStateException("LZMA2Option configuration failed", e);
                }
                FinishableWrapperOutputStream wrapped = new FinishableWrapperOutputStream(out);
                final org.tukaani.xz.FinishableOutputStream lzma2 = options.getOutputStream(wrapped);
                return new FinishableOutputStreamAdapter(lzma2) {
                    @Override
                    public void finish() throws IOException {
                        lzma2.finish();
                        IOUtils.finishStream(out);
                    }
                };
            }
            case lzma2_l5: {
                final LZMA2Options options;
                try {
                    options = new LZMA2Options(5);
                } catch (UnsupportedOptionsException e) {
                    throw new IllegalStateException("LZMA2Option configuration failed", e);
                }
                FinishableWrapperOutputStream wrapped = new FinishableWrapperOutputStream(out);
                final org.tukaani.xz.FinishableOutputStream lzma2 = options.getOutputStream(wrapped);
                return new FinishableOutputStreamAdapter(lzma2) {
                    @Override
                    public void finish() throws IOException {
                        lzma2.finish();
                        IOUtils.finishStream(out);
                    }
                };
            }
            default:
                throw new UnsupportedOperationException("Unsupported compression algorithm: "
                        + algo);
        }
    }
}
