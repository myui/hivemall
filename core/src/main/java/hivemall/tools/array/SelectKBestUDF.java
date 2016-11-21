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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Description(name = "select_k_best",
        value = "_FUNC_(array<number> array, const array<number> importance_list, const int k)"
                + " - Returns selected top-k elements as array<double>")
public class SelectKBestUDF extends GenericUDF {
    private ListObjectInspector featuresOI;
    private PrimitiveObjectInspector featureOI;
    private ListObjectInspector importanceListOI;
    private PrimitiveObjectInspector importanceOI;
    private PrimitiveObjectInspector kOI;

    private int[] topKIndices;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 3) {
            throw new UDFArgumentLengthException("Specify three arguments.");
        }

        if (!HiveUtils.isNumberListOI(OIs[0])) {
            throw new UDFArgumentTypeException(0,
                "Only array<number> type argument is acceptable but " + OIs[0].getTypeName()
                        + " was passed as `features`");
        }
        if (!HiveUtils.isNumberListOI(OIs[1])) {
            throw new UDFArgumentTypeException(1,
                "Only array<number> type argument is acceptable but " + OIs[1].getTypeName()
                        + " was passed as `importance_list`");
        }
        if (!HiveUtils.isIntegerOI(OIs[2])) {
            throw new UDFArgumentTypeException(2, "Only int type argument is acceptable but "
                    + OIs[2].getTypeName() + " was passed as `k`");
        }

        featuresOI = HiveUtils.asListOI(OIs[0]);
        featureOI = HiveUtils.asDoubleCompatibleOI(featuresOI.getListElementObjectInspector());
        importanceListOI = HiveUtils.asListOI(OIs[1]);
        importanceOI = HiveUtils.asDoubleCompatibleOI(importanceListOI.getListElementObjectInspector());
        kOI = HiveUtils.asIntegerOI(OIs[2]);

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        final double[] features = HiveUtils.asDoubleArray(dObj[0].get(), featuresOI, featureOI);
        final double[] importanceList = HiveUtils.asDoubleArray(dObj[1].get(), importanceListOI,
            importanceOI);
        final int k = PrimitiveObjectInspectorUtils.getInt(dObj[2].get(), kOI);

        Preconditions.checkNotNull(features);
        Preconditions.checkNotNull(importanceList);
        Preconditions.checkArgument(features.length == importanceList.length);
        Preconditions.checkArgument(features.length >= k);

        if (topKIndices == null) {
            final List<Map.Entry<Integer, Double>> list = new ArrayList<Map.Entry<Integer, Double>>();
            for (int i = 0; i < importanceList.length; i++) {
                list.add(new AbstractMap.SimpleEntry<Integer, Double>(i, importanceList[i]));
            }
            Collections.sort(list, new Comparator<Map.Entry<Integer, Double>>() {
                @Override
                public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {
                    return o1.getValue() > o2.getValue() ? -1 : 1;
                }
            });

            topKIndices = new int[k];
            for (int i = 0; i < k; i++) {
                topKIndices[i] = list.get(i).getKey();
            }
        }

        final List<DoubleWritable> result = new ArrayList<DoubleWritable>();
        for (int idx : topKIndices) {
            result.add(new DoubleWritable(features[idx]));
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        final StringBuilder sb = new StringBuilder();
        sb.append("select_k_best");
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
