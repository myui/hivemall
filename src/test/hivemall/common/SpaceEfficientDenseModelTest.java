package hivemall.common;

import static junit.framework.Assert.assertEquals;
import hivemall.io.DenseModel;
import hivemall.io.SpaceEfficientDenseModel;
import hivemall.io.WeightValue;
import hivemall.utils.collections.IMapIterator;

import java.util.Random;

import org.junit.Test;

public class SpaceEfficientDenseModelTest {

    @Test
    public void testGetSet() {
        final int size = 1 << 12;

        final SpaceEfficientDenseModel model1 = new SpaceEfficientDenseModel(size);
        model1.configureClock();
        final DenseModel model2 = new DenseModel(size);
        model2.configureClock();

        final Random rand = new Random();
        for(int t = 0; t < 1000; t++) {
            int i = rand.nextInt(size);
            float w = 65520f * rand.nextFloat();
            model1._set(i, w, (short) 1);
            model2._set(i, w, (short) 1);
        }

        assertEquals(model2.size(), model1.size());

        IMapIterator<Integer, WeightValue> itor = model1.entries();
        while(itor.next() != -1) {
            int k = itor.getKey();
            float expected = itor.getValue().get();
            float actual = model2.getWeight(k);
            assertEquals(expected, actual, 32f);
        }
    }

}
