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
import java.util.Map;

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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * A wrapper of [[hivemall.ftvec.SortByFeatureUDF]].
 *
 * NOTE: This is needed to avoid the issue of Spark reflection. That is, spark cannot handle Map<>
 * as a return type in Hive UDF. Therefore, the type must be passed via ObjectInspector.
 */
@Description(name = "sort_by_feature",
        value = "_FUNC_(map in map<int,float>) - Returns a sorted map")
@UDFType(deterministic = true, stateful = false)
public class SortByFeatureUDFWrapper extends GenericUDF {
    private SortByFeatureUDF udf = new SortByFeatureUDF();
    private MapObjectInspector argumentOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(
                "sorted_by_feature() has an single arguments: map<int, float> map");
        }

        switch (arguments[0].getCategory()) {
            case MAP:
                argumentOI = (MapObjectInspector) arguments[0];
                ObjectInspector keyOI = argumentOI.getMapKeyObjectInspector();
                ObjectInspector valueOI = argumentOI.getMapValueObjectInspector();
                if (keyOI.getCategory().equals(Category.PRIMITIVE)
                        && valueOI.getCategory().equals(Category.PRIMITIVE)) {
                    final PrimitiveCategory keyCategory = ((PrimitiveObjectInspector) keyOI).getPrimitiveCategory();
                    final PrimitiveCategory valueCategory = ((PrimitiveObjectInspector) valueOI).getPrimitiveCategory();
                    if (keyCategory == PrimitiveCategory.INT
                            && valueCategory == PrimitiveCategory.FLOAT) {
                        break;
                    }
                }
            default:
                throw new UDFArgumentTypeException(0, "Type mismatch: map");
        }


        return ObjectInspectorFactory.getStandardMapObjectInspector(
            argumentOI.getMapKeyObjectInspector(), argumentOI.getMapValueObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);
        final Object arrayObject = arguments[0].get();
        final MapObjectInspector arrayOI = argumentOI;
        @SuppressWarnings("unchecked")
        final Map<IntWritable, FloatWritable> input = (Map<IntWritable, FloatWritable>) argumentOI.getMap(arguments[0].get());
        return udf.evaluate(input);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "sort_by_feature(" + Arrays.toString(children) + ")";
    }
}
