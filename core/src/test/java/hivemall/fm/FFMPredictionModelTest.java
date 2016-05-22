/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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

import hivemall.fm.FieldAwareFactorizationMachineModel.Entry;
import hivemall.utils.collections.IntOpenHashTable;
import hivemall.utils.io.FastByteArrayInputStream;
import hivemall.utils.io.FastByteArrayOutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

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

    @Test
    public void testReadWriteStates() throws IOException, ClassNotFoundException {
        final Random rand = new Random(43);
        final int size = 100000;
        final byte[] expected = new byte[size];
        for (int i = 0; i < size; i++) {
            final float r = rand.nextFloat();
            if (r < 0.7f) {
                expected[i] = IntOpenHashTable.FULL;
            } else {
                expected[i] = IntOpenHashTable.FREE;
            }
        }

        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        FFMPredictionModel.writeStates(expected, new DataOutputStream(out));
        byte[] serialized = out.toByteArray();
        Assert.assertTrue("serialized: " + serialized.length + ", original: " + expected.length,
            serialized.length < (expected.length * 0.31f));

        final byte[] actual = new byte[size];
        FastByteArrayInputStream in = new FastByteArrayInputStream(serialized);
        FFMPredictionModel.readStates(new DataInputStream(in), actual);

        Assert.assertArrayEquals(expected, actual);
    }

}
