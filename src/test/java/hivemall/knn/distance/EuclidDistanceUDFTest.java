package hivemall.knn.distance;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class EuclidDistanceUDFTest {

    @Test
    public void test1() {
        EuclidDistanceUDF udf = new EuclidDistanceUDF();
        List<Text> ftvec1 = Arrays.asList(new Text("1:1.0"), new Text("2:2.0"), new Text("3:3.0"));
        List<Text> ftvec2 = Arrays.asList(new Text("1:2.0"), new Text("2:4.0"), new Text("3:6.0"));
        DoubleWritable d = udf.evaluate(ftvec1, ftvec2);
        Assert.assertEquals(Math.sqrt(1.0 + 4.0 + 9.0), d.get(), 0.d);
    }

    @Test
    public void test2() {
        EuclidDistanceUDF udf = new EuclidDistanceUDF();
        List<Text> ftvec1 = Arrays.asList(new Text("1:1.0"), new Text("2:3.0"), new Text("3:3.0"));
        List<Text> ftvec2 = Arrays.asList(new Text("1:2.0"), new Text("3:6.0"));
        DoubleWritable d = udf.evaluate(ftvec1, ftvec2);
        Assert.assertEquals(Math.sqrt(1.0 + 9.0 + 9.0), d.get(), 0.d);
    }

}
