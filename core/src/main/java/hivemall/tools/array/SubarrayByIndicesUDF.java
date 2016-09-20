/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2016 Makoto YUI
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
package hivemall.tools.array;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Preconditions;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.ArrayList;
import java.util.List;

@Description(name = "subarray_by_indices",
        value = "_FUNC_(array<number> input, array<int> indices)"
                + " - Returns subarray selected by given indices as array<number>")
public class SubarrayByIndicesUDF extends GenericUDF {
    private ListObjectInspector inputOI;
    private PrimitiveObjectInspector elementOI;
    private ListObjectInspector indicesOI;
    private PrimitiveObjectInspector indexOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isListOI(OIs[0])) {
            throw new UDFArgumentTypeException(0,
                "Only array<number> type argument is acceptable but " + OIs[0].getTypeName()
                        + " was passed as `input`");
        }
        if (!HiveUtils.isListOI(OIs[1])
                || !HiveUtils.isIntegerOI(((ListObjectInspector) OIs[1]).getListElementObjectInspector())) {
            throw new UDFArgumentTypeException(0,
                "Only array<int> type argument is acceptable but " + OIs[0].getTypeName()
                        + " was passed as `indices`");
        }

        inputOI = HiveUtils.asListOI(OIs[0]);
        elementOI = HiveUtils.asDoubleCompatibleOI(inputOI.getListElementObjectInspector());
        indicesOI = HiveUtils.asListOI(OIs[1]);
        indexOI = HiveUtils.asIntegerOI(indicesOI.getListElementObjectInspector());

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        final double[] input = HiveUtils.asDoubleArray(dObj[0].get(), inputOI, elementOI);
        final List indices = indicesOI.getList(dObj[1].get());

        Preconditions.checkNotNull(input);
        Preconditions.checkNotNull(indices);

        List<DoubleWritable> result = new ArrayList<DoubleWritable>();
        for (Object indexObj : indices) {
            int index = PrimitiveObjectInspectorUtils.getInt(indexObj, indexOI);
            if (index > input.length - 1) {
                throw new ArrayIndexOutOfBoundsException(index);
            }

            result.add(new DoubleWritable(input[index]));
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("subarray_by_indices");
        sb.append("(");
        if (children.length > 0) {
            sb.append(children[0]);
            for (int i = 1; i < children.length; i++) {
                sb.append(", ");
                sb.append(children[i]);
            }
        }
        sb.append(")");
        return sb.toString();
    }
}
