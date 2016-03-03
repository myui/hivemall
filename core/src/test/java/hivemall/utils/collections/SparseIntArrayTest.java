package hivemall.utils.collections;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class SparseIntArrayTest {

    @Test
    public void testDense() {
        int size = 1000;
        Random rand = new Random(31);
        int[] expected = new int[size];
        IntArray actual = new SparseIntArray(10);
        for (int i = 0; i < size; i++) {
            int r = rand.nextInt(size);
            expected[i] = r;
            actual.put(i, r);
        }
        for (int i = 0; i < size; i++) {
            Assert.assertEquals(expected[i], actual.get(i));
        }
    }

    @Test
    public void testSparse() {
        int size = 1000;
        Random rand = new Random(31);
        int[] expected = new int[size];
        SparseIntArray actual = new SparseIntArray(10);
        for (int i = 0; i < size; i++) {
            int key = rand.nextInt(size);
            int v = rand.nextInt();
            expected[key] = v;
            actual.put(key, v);
        }
        for (int i = 0; i < actual.size(); i++) {
            int key = actual.keyAt(i);
            Assert.assertEquals(expected[key], actual.get(key, 0));
        }
    }
}
