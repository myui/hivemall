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
package hivemall.utils.codec;

import static java.util.zip.Deflater.DEFAULT_COMPRESSION;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.annotation.Nonnull;

public final class DeflateCodec extends CompressionCodec {

    private final Deflater compressor;
    private final Inflater decompressor;

    public DeflateCodec() {
        this(true, true);
    }

    public DeflateCodec(boolean compress, boolean decompress) {
        super();
        this.compressor = compress ? new Deflater(DEFAULT_COMPRESSION, true) : null;
        this.decompressor = decompress ? new Inflater(true) : null;
    }

    @Override
    public byte[] compress(@Nonnull byte[] in, int off, int len) throws IOException {
        return compress(in, off, len, DEFAULT_COMPRESSION);
    }

    @Nonnull
    public byte[] compress(@Nonnull final byte[] in, final int off, final int len, final int level)
            throws IOException {
        // Create an expandable byte array to hold the compressed data.
        byte[] compressedData = new byte[len];

        int compressedSize;
        try {
            compressor.reset();
            if (level != DEFAULT_COMPRESSION) {
                compressor.setLevel(level);
            }
            // Give the compressor the data to compress
            compressor.setInput(in, off, len);
            compressor.finish();
            compressedSize = compressor.deflate(compressedData);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        final int header;
        if (compressedSize == 0) {
            compressedData = in;
            compressedSize = len;
            header = 0;
        } else if (compressedSize >= (len - 4)) {
            compressedData = in;
            compressedSize = len;
            header = 0;
        } else {
            header = len;
        }
        final byte[] output = new byte[compressedSize + 4];
        output[0] = (byte) (header >> 24);
        output[1] = (byte) (header >> 16);
        output[2] = (byte) (header >> 8);
        output[3] = (byte) header;
        System.arraycopy(compressedData, 0, output, 4, compressedSize);
        return output;
    }

    @Override
    public byte[] decompress(@Nonnull final byte[] in, final int off, final int len)
            throws IOException {
        final int originalSize = (((in[off] & 0xff) << 24) + ((in[off + 1] & 0xff) << 16)
                + ((in[off + 2] & 0xff) << 8) + (in[off + 3] & 0xff));
        if (originalSize == 0) {
            byte[] dest = new byte[len - 4];
            System.arraycopy(in, 4, dest, 0, len - 4);
            return dest;
        }
        // Create an expandable byte array to hold the decompressed data
        final byte[] result = new byte[originalSize];
        try {
            // Decompress the data
            decompressor.reset();
            decompressor.setInput(in, 4, len - 4);
            decompressor.inflate(result);
        } catch (DataFormatException dfe) {
            throw new IOException(dfe);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (compressor != null) {
            compressor.end();
        }
        if (decompressor != null) {
            decompressor.end();
        }
    }

}
