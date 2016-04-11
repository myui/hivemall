/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hivemall.ftvec;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

/**
 * A wrapper of [[hivemall.ftvec.AddBiasUDF]].
 *
 * NOTE: This is needed to avoid the issue of Spark reflection.
 * That is, spark cannot handle List<> as a return type in Hive UDF.
 * Therefore, the type must be passed via ObjectInspector.
 */
@Description(name = "add_bias", value = "_FUNC_(features in array<string>) - Returns features with a bias as array<string>")
@UDFType(deterministic = true, stateful = false)
public class AddBiasUDFWrapper extends GenericUDF {
    private AddBiasUDF udf = new AddBiasUDF();
    private ListObjectInspector argumentOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 1) {
            throw new UDFArgumentLengthException(
                "add_bias() has an single arguments: array<string> features");
        }

        switch(arguments[0].getCategory()) {
            case LIST:
                argumentOI = (ListObjectInspector) arguments[0];
                ObjectInspector elmOI = argumentOI.getListElementObjectInspector();
                if(elmOI.getCategory().equals(Category.PRIMITIVE)) {
                    if (((PrimitiveObjectInspector) elmOI).getPrimitiveCategory()
                            == PrimitiveCategory.STRING) {
                        break;
                    }
                }
            default:
                throw new UDFArgumentTypeException(0, "Type mismatch: features");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(
                argumentOI.getListElementObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert(arguments.length == 1);
        @SuppressWarnings("unchecked")
        final List<String> input = (List<String>) argumentOI.getList(arguments[0].get());
        return udf.evaluate(input);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "add_bias(" + Arrays.toString(children) + ")";
    }
}
