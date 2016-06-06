package hivemall.fm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class FactorizationMachineUDTFTest {
    private static final boolean DEBUG = false;
    private static final int ITERATIONS = 50;

    @Test
    public void testSGD() throws HiveException, IOException {
        println("SGD test");
        FactorizationMachineUDTF udtf = new FactorizationMachineUDTF();
        ObjectInspector[] argOIs = new ObjectInspector[] {
                ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
                ObjectInspectorUtils.getConstantObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    "-factors 5 -min 1 -max 5 -iters 1 -init_v gaussian -eta0 0.01 -seed 31")};

        udtf.initialize(argOIs);
        FactorizationMachineModel model = udtf.getModel();
        Assert.assertTrue("Actual class: " + model.getClass().getName(),
            model instanceof FMStringFeatureMapModel);

        double loss = 0.d;
        double cumul = 0.d;
        for (int trainingIteration = 1; trainingIteration <= ITERATIONS; ++trainingIteration) {
            BufferedReader data = new BufferedReader(new InputStreamReader(
                getClass().getResourceAsStream("5107786.txt")));
            loss = udtf._cvState.getCumulativeLoss();
            int trExamples = 0;
            String line = data.readLine();
            while (line != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, " ");
                double y = Double.parseDouble(tokenizer.nextToken());
                List<String> features = new ArrayList<String>();
                while (tokenizer.hasMoreTokens()) {
                    String f = tokenizer.nextToken();
                    features.add(f);
                }
                udtf.process(new Object[] {features, y});
                trExamples++;
                line = data.readLine();
            }
            cumul = udtf._cvState.getCumulativeLoss();
            loss = (cumul - loss) / trExamples;
            println(trainingIteration + " " + loss + " " + cumul / (trainingIteration * trExamples));
            data.close();
        }
        Assert.assertTrue("Loss was greater than 0.1: " + loss, loss <= 0.1);

    }

    private static void println(String line) {
        if (DEBUG) {
            System.out.println(line);
        }
    }

}
