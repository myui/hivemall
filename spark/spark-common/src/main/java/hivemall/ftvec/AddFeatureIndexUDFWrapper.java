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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * A wrapper of [[hivemall.ftvec.AddFeatureIndexUDF]].
 *
 * NOTE: This is needed to avoid the issue of Spark reflection. That is, spark cannot handle List<>
 * as a return type in Hive UDF. Therefore, the type must be passed via ObjectInspector.
 */
@Description(
        name = "add_feature_index",
        value = "_FUNC_(dense features in array<double>) - Returns a feature vector with feature indicies")
@UDFType(deterministic = true, stateful = false)
public class AddFeatureIndexUDFWrapper extends GenericUDF {
    private AddFeatureIndexUDF udf = new AddFeatureIndexUDF();
    private ListObjectInspector argumentOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                "add_feature_index() has an single arguments: array<double> features");
        }

        switch (arguments[0].getCategory()) {
            case LIST:
                argumentOI = (ListObjectInspector) arguments[0];
                ObjectInspector elmOI = argumentOI.getListElementObjectInspector();
                if (elmOI.getCategory().equals(Category.PRIMITIVE)) {
                    if (((PrimitiveObjectInspector) elmOI).getPrimitiveCategory() == PrimitiveCategory.DOUBLE) {
                        break;
                    }
                }
            default:
                throw new UDFArgumentTypeException(0, "Type mismatch: features");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);
        @SuppressWarnings("unchecked")
        final List<Double> input = (List<Double>) argumentOI.getList(arguments[0].get());
        return udf.evaluate(input);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "add_feature_index(" + Arrays.toString(children) + ")";
    }
}
