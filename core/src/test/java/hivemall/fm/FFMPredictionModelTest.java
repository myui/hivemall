package hivemall.fm;

import hivemall.fm.FieldAwareFactorizationMachineModel.Entry;
import hivemall.utils.collections.IntOpenHashTable;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class FFMPredictionModelTest {

    @Test
    public void testSerialize() throws IOException, ClassNotFoundException {
        IntOpenHashTable<Entry> map = new IntOpenHashTable<FieldAwareFactorizationMachineModel.Entry>(
            100);
        map.put(1, new Entry(1, new float[] {1f, -1f, -1f}));
        map.put(2, new Entry(2, new float[] {1f, 2f, -1f}));
        map.put(3, new Entry(3, new float[] {1f, 2f, 3f}));
        FFMPredictionModel expected = new FFMPredictionModel(map, 0d, 3,
            Feature.DEFAULT_NUM_FEATURES, Feature.DEFAULT_NUM_FIELDS);
        byte[] b = expected.serialize();

        FFMPredictionModel actual = FFMPredictionModel.deserialize(b, b.length);
        Assert.assertEquals(3, actual.getNumFactors());
        Assert.assertEquals(Feature.DEFAULT_NUM_FEATURES, actual.getNumFeatures());
        Assert.assertEquals(Feature.DEFAULT_NUM_FIELDS, actual.getNumFields());
    }

}
