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
package hivemall.ftvec.scaling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * A wrapper of [[hivemall.ftvec.scaling.L2NormalizationUDF]].
 *
 * NOTE: This is needed to avoid the issue of Spark reflection. That is, spark-1.3 cannot handle
 * List<> as a return type in Hive UDF. The type must be passed via ObjectInspector. This issues has
 * been reported in SPARK-6747, so a future release of Spark makes the wrapper obsolete.
 */
public class L2NormalizationUDFWrapper extends GenericUDF {
    private L2NormalizationUDF udf = new L2NormalizationUDF();

    private transient List<Text> retValue = new ArrayList<Text>();
    private transient Converter toListText = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("normalize() has an only single argument.");
        }

        switch (arguments[0].getCategory()) {
            case LIST:
                ObjectInspector elmOI = ((ListObjectInspector) arguments[0]).getListElementObjectInspector();
                if (elmOI.getCategory().equals(Category.PRIMITIVE)) {
                    if (((PrimitiveObjectInspector) elmOI).getPrimitiveCategory() == PrimitiveCategory.STRING) {
                        break;
                    }
                }
            default:
                throw new UDFArgumentTypeException(0,
                    "normalize() must have List[String] as an argument, but "
                            + arguments[0].getTypeName() + " was found.");
        }

        // Create a ObjectInspector converter for arguments
        ObjectInspector outputElemOI = ObjectInspectorFactory.getReflectionObjectInspector(
            Text.class, ObjectInspectorOptions.JAVA);
        ObjectInspector outputOI = ObjectInspectorFactory.getStandardListObjectInspector(outputElemOI);
        toListText = ObjectInspectorConverters.getConverter(arguments[0], outputOI);

        ObjectInspector listElemOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector returnElemOI = ObjectInspectorUtils.getStandardObjectInspector(listElemOI);
        return ObjectInspectorFactory.getStandardListObjectInspector(returnElemOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);
        @SuppressWarnings("unchecked")
        final List<Text> input = (List<Text>) toListText.convert(arguments[0].get());
        retValue = udf.evaluate(input);
        return retValue;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "normalize(" + Arrays.toString(children) + ")";
    }
}
