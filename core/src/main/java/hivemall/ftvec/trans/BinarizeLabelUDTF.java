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

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(name = "binarize_label", value = "_FUNC_(int/long positive, int/long negative, ...) "
        + "- Returns positive/negative records that are represented "
        + "as (..., int label) where label is 0 or 1")
@UDFType(deterministic = true, stateful = false)
public final class BinarizeLabelUDTF extends GenericUDTF {

    private PrimitiveObjectInspector positiveOI;
    private PrimitiveObjectInspector negativeOI;
    private Object[] positiveObjs;
    private Object[] negativeObjs;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length < 3) {
            throw new UDFArgumentException("binalize_label(int/long positive, "
                    + "int/long negative, *) takes at least three arguments");
        }
        this.positiveOI = HiveUtils.asIntCompatibleOI(argOIs[0]);
        this.negativeOI = HiveUtils.asIntCompatibleOI(argOIs[1]);

        this.positiveObjs = new Object[argOIs.length - 1];
        positiveObjs[positiveObjs.length - 1] = 0;
        this.negativeObjs = new Object[argOIs.length - 1];
        negativeObjs[negativeObjs.length - 1] = 1;

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        for (int i = 2; i < argOIs.length; i++) {
            fieldNames.add("c" + (i - 2));
            // Use negative label ObjectInspector here. OIs for positive
            // label and negative labels must be same.
            fieldOIs.add(argOIs[i]);
        }
        fieldNames.add("c" + (argOIs.length - 2));
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final Object[] positiveObjs = this.positiveObjs;
        for (int i = 0, last = positiveObjs.length - 1; i < last; i++) {
            positiveObjs[i] = args[i + 2];
        }
        // Forward positive label
        final int positive = PrimitiveObjectInspectorUtils.getInt(args[0], positiveOI);
        for (int i = 0; i < positive; i++) {
            forward(positiveObjs);
        }

        final Object[] negativeObjs = this.negativeObjs;
        for (int i = 0, last = negativeObjs.length - 1; i < last; i++) {
            negativeObjs[i] = args[i + 2];
        }
        // Forward negative label
        final int negative = PrimitiveObjectInspectorUtils.getInt(args[1], negativeOI);
        for (int i = 0; i < negative; i++) {
            forward(negativeObjs);
        }
    }

    @Override
    public void close() throws HiveException {
        this.positiveObjs = null;
        this.negativeObjs = null;
    }
}
