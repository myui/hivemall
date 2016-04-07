/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.fm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class FieldAwareFactorizationMachineUDTFTest {

    @Test
    public void test() throws HiveException, IOException {
        FieldAwareFactorizationMachineUDTF udtf = new FieldAwareFactorizationMachineUDTF();
        ObjectInspector[] argOIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
                ObjectInspectorUtils.getConstantObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-factor 10")};

        udtf.initialize(argOIs);
        FactorizationMachineModel model = udtf.getModel();
        Assert.assertTrue("Actual class: " + model.getClass().getName(),
            model instanceof FFMStringFeatureMapModel);

        BufferedReader data = new BufferedReader(new InputStreamReader(
            getClass().getResourceAsStream("bigdata.tr.txt")));
        double loss = udtf._cvState.getCumulativeLoss();
        for (int i = 0; i < 1000; ++i) {
            //gather features in current line
            final String input = data.readLine();
            if (input == null) {
                System.out.println("EOF reached");
                break;
            }
            ArrayList<String> featureStrings = new ArrayList<String>();
            ArrayList<StringFeature> features = new ArrayList<StringFeature>();

            //make StringFeature for each word = data point
            String remaining = input;
            int wordCut = remaining.indexOf(' ');
            while (wordCut != -1) {
                featureStrings.add(remaining.substring(0, wordCut));
                remaining = remaining.substring(wordCut + 1);
                wordCut = remaining.indexOf(' ');
            }
            int end = featureStrings.size();
            double y = Double.parseDouble(featureStrings.get(0));
            if (y == 0) {
                y = -1;//LibFFM data uses {0, 1}; Hivemall uses {-1, 1}
            }
            for (int j = 1; j < end; ++j) {
                String entireFeature = featureStrings.get(j);
                int featureCut = StringUtils.ordinalIndexOf(entireFeature, ":", 2);
                String feature = entireFeature.substring(0, featureCut);
                double value = Double.parseDouble(entireFeature.substring(featureCut + 1));
                features.add(new StringFeature(feature, value));
            }
            udtf.process(new Object[] {toStringArray(features), y});
            loss = udtf._cvState.getCumulativeLoss() - loss;
            System.out.println("loss for this iteration: " + loss
                    + "; average loss up to this iteration: " + udtf._cvState.getCumulativeLoss()
                    / i);
            loss = udtf._cvState.getCumulativeLoss();
        }
    }

    private static String[] toStringArray(ArrayList<StringFeature> x) {
        final int size = x.size();
        final String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = x.get(i).toString();
        }
        return ret;
    }

}
