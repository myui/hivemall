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
package hivemall.ftvec.trans;

import hivemall.utils.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(
        name = "indexed_features",
        value = "_FUNC_(double v1, double v2, ...) - Returns a list of features as array<string>: [1:v1, 2:v2, ..]")
@UDFType(deterministic = true, stateful = false)
public final class IndexedFeatures extends GenericUDF {

    // KryoException java.lang.NullPointerException if initialized in {@link #initialize(ObjectInspector[])}
    // serialized and sent to mappers/reducers
    private List<String> list;

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        int numArgs = argOIs.length;
        if (numArgs < 1) {
            throw new UDFArgumentLengthException(
                "features(v1, ..) requires at least 1 arguments, got " + argOIs.length);
        }

        this.list = null;
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public List<String> evaluate(DeferredObject[] arguments) throws HiveException {
        final int size = arguments.length;
        if (list == null) {
            this.list = new ArrayList<String>(size);
        } else {
            list.clear();
        }

        final StringBuilder buf = new StringBuilder(64);
        for (int i = 0; i < size; i++) {
            Object o = arguments[i].get();
            if (o == null) {
                continue;
            }
            String s1 = o.toString();
            if (s1.isEmpty()) {
                continue;
            }
            String s2 = buf.append(i + 1).append(':').append(s1).toString();
            list.add(s2);
            StringUtils.clear(buf);
        }

        return list;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "indexed_features(" + Arrays.toString(children) + ")";
    }

}
