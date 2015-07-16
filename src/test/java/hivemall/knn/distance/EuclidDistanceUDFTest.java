package hivemall.knn.distance;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.junit.Assert;
import org.junit.Test;

public class EuclidDistanceUDFTest {

    @Test
    public void test1() {
        EuclidDistanceUDF udf = new EuclidDistanceUDF();
        List<String> ftvec1 = Arrays.asList("1:1.0", "2:2.0", "3:3.0");
        List<String> ftvec2 = Arrays.asList("1:2.0", "2:4.0", "3:6.0");
        FloatWritable d = udf.evaluate(ftvec1, ftvec2);
        Assert.assertEquals((float) Math.sqrt(1.0 + 4.0 + 9.0), d.get(), 0.f);
    }

    @Test
    public void test2() {
        EuclidDistanceUDF udf = new EuclidDistanceUDF();
        List<String> ftvec1 = Arrays.asList("1:1.0", "2:3.0", "3:3.0");
        List<String> ftvec2 = Arrays.asList("1:2.0", "3:6.0");
        FloatWritable d = udf.evaluate(ftvec1, ftvec2);
        Assert.assertEquals((float) Math.sqrt(1.0 + 9.0 + 9.0), d.get(), 0.f);
    }

}
