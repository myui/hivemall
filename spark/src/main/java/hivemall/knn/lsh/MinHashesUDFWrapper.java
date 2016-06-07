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

package hivemall.knn.lsh;

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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

/** A wrapper of [[hivemall.knn.lsh.MinHashesUDF]]. */
@Description(
        name = "minhashes",
        value = "_FUNC_(features in array<string>, noWeight in boolean) - Returns hashed features as array<int>")
@UDFType(deterministic = true, stateful = false)
public class MinHashesUDFWrapper extends GenericUDF {
    private MinHashesUDF udf = new MinHashesUDF();
    private ListObjectInspector featuresOI = null;
    private PrimitiveObjectInspector noWeightOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                "minhashes() has 2 arguments: array<string> features, boolean noWeight");
        }

        // Check argument types
        switch (arguments[0].getCategory()) {
            case LIST:
                featuresOI = (ListObjectInspector) arguments[0];
                ObjectInspector elmOI = featuresOI.getListElementObjectInspector();
                if (elmOI.getCategory().equals(Category.PRIMITIVE)) {
                    if (((PrimitiveObjectInspector) elmOI).getPrimitiveCategory() == PrimitiveCategory.STRING) {
                        break;
                    }
                }
            default:
                throw new UDFArgumentTypeException(0, "Type mismatch: features");
        }

        noWeightOI = (PrimitiveObjectInspector) arguments[1];
        if (noWeightOI.getPrimitiveCategory() != PrimitiveCategory.BOOLEAN) {
            throw new UDFArgumentException("Type mismatch: noWeight");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveCategory.INT));
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 2);
        @SuppressWarnings("unchecked")
        final List<String> features = (List<String>) featuresOI.getList(arguments[0].get());
        final Boolean noWeight = PrimitiveObjectInspectorUtils.getBoolean(arguments[1].get(),
            noWeightOI);
        return udf.evaluate(features, noWeight);
    }

    @Override
    public String getDisplayString(String[] children) {
        /**
         * TODO: Need to return hive-specific type names.
         */
        return "minhashes(" + Arrays.toString(children) + ")";
    }
}
