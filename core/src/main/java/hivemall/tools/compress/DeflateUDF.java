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
package hivemall.tools.compress;

import hivemall.utils.codec.DeflateCodec;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.IOUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.zip.Deflater;

import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@Description(name = "deflate", value = "_FUNC_(TEXT data [, const int compressionLevel]) - "
        + "Returns a compressed BINARY obeject by using Deflater.",
        extended = "The compression level must be in range [-1,9]")
@UDFType(deterministic = true, stateful = false)
public final class DeflateUDF extends GenericUDF {

    private StringObjectInspector stringOI;
    private int compressionLevel;

    @Nullable
    private transient DeflateCodec codec;
    @Nullable
    private transient BytesWritable result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1 && argOIs.length != 2) {
            throw new UDFArgumentException("_FUNC_ takes 1 or 2 arguments");
        }
        this.stringOI = HiveUtils.asStringOI(argOIs[0]);

        int level = Deflater.DEFAULT_COMPRESSION;
        if (argOIs.length == 2) {
            level = HiveUtils.getConstInt(argOIs[1]);
            if ((level < 0 || level > 9) && level != Deflater.DEFAULT_COMPRESSION) {
                throw new UDFArgumentException("Invalid compression level: " + level);
            }
        }
        this.compressionLevel = level;

        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    }

    @Override
    public BytesWritable evaluate(DeferredObject[] arguments) throws HiveException {
        if (codec == null) {
            this.codec = new DeflateCodec(true, false);
        }

        Object arg0 = arguments[0].get();
        if (arg0 == null) {
            return null;
        }
        Text text = stringOI.getPrimitiveWritableObject(arg0);
        byte[] original = text.getBytes();
        final int len = text.getLength();
        final byte[] compressed;
        try {
            compressed = codec.compress(original, 0, len, compressionLevel);
        } catch (IOException e) {
            throw new HiveException("Failed to compress", e);
        }
        original = null;
        if (result == null) {
            this.result = new BytesWritable(compressed);
        } else {
            result.set(compressed, 0, compressed.length);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(codec);
        this.codec = null;
        this.result = null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "deflate(" + Arrays.toString(children) + ")";
    }

}
