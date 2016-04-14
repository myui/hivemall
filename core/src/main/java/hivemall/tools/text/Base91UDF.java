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
package hivemall.tools.text;

import hivemall.utils.compress.Base91;
import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.io.FastByteArrayOutputStream;

import java.io.IOException;
import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

@Description(name = "base91",
        value = "_FUNC_(BINARY bin) - Convert the argument from binary to a BASE91 string")
@UDFType(deterministic = true, stateful = false)
public final class Base91UDF extends GenericUDF {

    private BinaryObjectInspector binaryOI;

    @Nonnull
    private transient FastByteArrayOutputStream outputBuf;
    @Nonnull
    private transient Text result;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentException("_FUNC_ takes exactly 1 argument");
        }
        this.binaryOI = HiveUtils.asBinaryOI(argOIs[0]);

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Text evaluate(DeferredObject[] arguments) throws HiveException {
        if (outputBuf == null) {
            this.outputBuf = new FastByteArrayOutputStream(4096);
        } else {
            outputBuf.reset();
        }

        Object arg0 = arguments[0].get();
        if (arg0 == null) {
            return null;
        }

        BytesWritable input = binaryOI.getPrimitiveWritableObject(arg0);
        final byte[] inputBytes = input.getBytes();
        final int len = input.getLength();
        try {
            Base91.encode(inputBytes, 0, len, outputBuf);
        } catch (IOException e) {
            throw new HiveException(e);
        }

        if (result == null) {
            byte[] outputBytes = outputBuf.toByteArray();
            this.result = new Text(outputBytes);
        } else {
            byte[] outputBytes = outputBuf.getInternalArray();
            int outputSize = outputBuf.size();
            result.set(outputBytes, 0, outputSize);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        this.outputBuf = null;
        this.result = null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "base91(" + Arrays.toString(children) + ')';
    }

}
