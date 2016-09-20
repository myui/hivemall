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
package hivemall.ftvec.selection;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.lang.Preconditions;
import hivemall.utils.math.StatsUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Description(name = "chi2",
        value = "_FUNC_(array<array<number>> observed, array<array<number>> expected)"
                + " - Returns chi2_val and p_val of each columns as <array<double>, array<double>>")
public class ChiSquareUDF extends GenericUDF {
    private ListObjectInspector observedOI;
    private ListObjectInspector observedRowOI;
    private PrimitiveObjectInspector observedElOI;
    private ListObjectInspector expectedOI;
    private ListObjectInspector expectedRowOI;
    private PrimitiveObjectInspector expectedElOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] OIs) throws UDFArgumentException {
        if (OIs.length != 2) {
            throw new UDFArgumentLengthException("Specify two arguments.");
        }

        if (!HiveUtils.isNumberListListOI(OIs[0])) {
            throw new UDFArgumentTypeException(0,
                "Only array<array<number>> type argument is acceptable but " + OIs[0].getTypeName()
                        + " was passed as `observed`");
        }

        if (!HiveUtils.isNumberListListOI(OIs[1])) {
            throw new UDFArgumentTypeException(1,
                "Only array<array<number>> type argument is acceptable but " + OIs[1].getTypeName()
                        + " was passed as `expected`");
        }

        observedOI = HiveUtils.asListOI(OIs[1]);
        observedRowOI = HiveUtils.asListOI(observedOI.getListElementObjectInspector());
        observedElOI = HiveUtils.asDoubleCompatibleOI(observedRowOI.getListElementObjectInspector());
        expectedOI = HiveUtils.asListOI(OIs[0]);
        expectedRowOI = HiveUtils.asListOI(expectedOI.getListElementObjectInspector());
        expectedElOI = HiveUtils.asDoubleCompatibleOI(expectedRowOI.getListElementObjectInspector());

        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

        return ObjectInspectorFactory.getStandardStructObjectInspector(
            Arrays.asList("chi2_vals", "p_vals"), fieldOIs);
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] dObj) throws HiveException {
        List observedObj = observedOI.getList(dObj[0].get()); // shape = (#classes, #features)
        List expectedObj = expectedOI.getList(dObj[1].get()); // shape = (#classes, #features)

        Preconditions.checkNotNull(observedObj);
        Preconditions.checkNotNull(expectedObj);
        final int nClasses = observedObj.size();
        Preconditions.checkArgument(nClasses == expectedObj.size()); // same #rows

        int nFeatures = -1;
        double[] observedRow = null; // to reuse
        double[] expectedRow = null; // to reuse
        double[][] observed = null; // shape = (#features, #classes)
        double[][] expected = null; // shape = (#features, #classes)

        // explode and transpose matrix
        for (int i = 0; i < nClasses; i++) {
            if (i == 0) {
                // init
                observedRow = HiveUtils.asDoubleArray(observedObj.get(i), observedRowOI,
                    observedElOI, false);
                expectedRow = HiveUtils.asDoubleArray(expectedObj.get(i), expectedRowOI,
                    expectedElOI, false);
                nFeatures = observedRow.length;
                observed = new double[nFeatures][nClasses];
                expected = new double[nFeatures][nClasses];
            } else {
                HiveUtils.toDoubleArray(observedObj.get(i), observedRowOI, observedElOI,
                    observedRow, false);
                HiveUtils.toDoubleArray(expectedObj.get(i), expectedRowOI, expectedElOI,
                    expectedRow, false);
            }

            for (int j = 0; j < nFeatures; j++) {
                observed[j][i] = observedRow[j];
                expected[j][i] = expectedRow[j];
            }
        }

        final Map.Entry<double[], double[]> chi2 = StatsUtils.chiSquares(observed, expected);

        final Object[] result = new Object[2];
        result[0] = WritableUtils.toWritableList(chi2.getKey());
        result[1] = WritableUtils.toWritableList(chi2.getValue());
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        final StringBuilder sb = new StringBuilder();
        sb.append("chi2");
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
