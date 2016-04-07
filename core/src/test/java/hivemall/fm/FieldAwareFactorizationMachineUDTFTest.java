package hivemall.fm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class FieldAwareFactorizationMachineUDTFTest {

    @Test
    public void test() {
        FieldAwareFactorizationMachineUDTF udtf = new FieldAwareFactorizationMachineUDTF();
        ObjectInspector[] argOIs = new ObjectInspector[] {
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
            ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, "-factor 10")
        };
        try {
            udtf.initialize(argOIs);
        } catch (UDFArgumentException e1) {
            e1.printStackTrace();
        }
        FactorizationMachineModel model = udtf.getModel();
        Assert.assertTrue("Actual class: " + model.getClass().getName(), model instanceof FFMStringFeatureMapModel);
        
        BufferedReader data;
        try {            
            data = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("bigdata.tr.txt")));
            double loss = udtf._cvState.getCumulativeLoss();
            for(int i = 0; i < 1000; ++i) {
                //gather features in current line
                final String input = data.readLine();
                if(input == null) {
                    System.out.println("EOF reached");
                    break;
                }
                ArrayList<String> featureStrings = new ArrayList<String>();
                ArrayList<StringFeature> features = new ArrayList<StringFeature>();
                
                //make StringFeature for each word = data point
                String remaining = input;
                int wordCut = remaining.indexOf(' ');
                while(wordCut != -1) {
                    featureStrings.add(remaining.substring(0, wordCut));
                    remaining = remaining.substring(wordCut + 1);
                    wordCut = remaining.indexOf(' ');
                }
                int end = featureStrings.size();
                double y = Double.parseDouble(featureStrings.get(0));
                if(y == 0) {
                    y = -1;//LibFFM data uses {0, 1}; Hivemall uses {-1, 1}
                }
                for (int j = 1; j < end; ++j) {
                    String entireFeature = featureStrings.get(j);
                    int featureCut = StringUtils.ordinalIndexOf(entireFeature, ":", 2);
                    String feature = entireFeature.substring(0, featureCut);
                    double value = Double.parseDouble(entireFeature.substring(featureCut + 1));
                    features.add(new StringFeature(feature, value));
                }
                udtf.process(new Object[] { toStringArray(features), y });
                loss = udtf._cvState.getCumulativeLoss() - loss;
                System.out.println("loss for this iteration: " + loss + "; average loss up to this iteration: " + udtf._cvState.getCumulativeLoss()/i);
                loss = udtf._cvState.getCumulativeLoss();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (HiveException e) {
            e.printStackTrace();
        }
    }
    
    private static String[] toStringArray(ArrayList<StringFeature> x) {
        final int size = x.size();
        final String[] ret = new String[size];
        for(int i = 0; i < size; i++) {
            ret[i] = x.get(i).toString();
        }
        return ret;
    }

}
