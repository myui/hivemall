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
package hivemall.xgboost.tools;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

@Description(
    name = "xgboost_multiclass_predict",
    value = "_FUNC_(string rowid, string[] features, string model_id, array<byte> pred_model [, string options]) "
                + "- Returns a prediction result as (string rowid, int label, float probability)"
)
public final class XGBoostMulticlassPredictUDTF extends hivemall.xgboost.XGBoostPredictUDTF {

    public XGBoostMulticlassPredictUDTF() {}

    /** Return (string rowid, int label, float probability) as a result */
    @Override
    public StructObjectInspector getReturnOI() {
        final ArrayList fieldNames = new ArrayList(3);
        final ArrayList fieldOIs = new ArrayList(3);
        fieldNames.add("rowid");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("label");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("probability");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void forwardPredicted(
            final List<LabeledPointWithRowId> testData,
            final float[][] predicted) throws HiveException {
        assert(predicted.length == testData.size());
        for(int i = 0; i < testData.size(); i++) {
            assert(predicted[i].length > 1);
            final String rowId = testData.get(i).rowId;
            for(int j = 0; j < predicted[i].length; j++) {
                float prob = predicted[i][j];
                forward(new Object[]{rowId, j, prob});
            }
        }
    }

}
