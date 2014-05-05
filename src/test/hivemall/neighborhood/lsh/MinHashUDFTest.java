package hivemall.neighborhood.lsh;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

public class MinHashUDFTest {

    @Test
    public void testEvaluate() throws HiveException {
        MinHashesUDF minhash = new MinHashesUDF();

        Assert.assertEquals(5, minhash.evaluate(Arrays.asList(1, 2, 3, 4)).size());
        Assert.assertEquals(9, minhash.evaluate(Arrays.asList(1, 2, 3, 4), 9, 2).size());

        Assert.assertEquals(minhash.evaluate(Arrays.asList(1, 2, 3, 4)), minhash.evaluate(Arrays.asList(1, 2, 3, 4), 5, 2));

        Assert.assertEquals(minhash.evaluate(Arrays.asList(1, 2, 3, 4)), minhash.evaluate(Arrays.asList("1", "2", "3", "4"), true));

        Assert.assertEquals(minhash.evaluate(Arrays.asList(1, 2, 3, 4)), minhash.evaluate(Arrays.asList("1:1.0", "2:1.0", "3:1.0", "4:1.0"), false));
    }
}
