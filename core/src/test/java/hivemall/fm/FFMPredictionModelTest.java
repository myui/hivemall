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

import hivemall.utils.buffer.HeapBuffer;
import hivemall.utils.collections.Int2LongOpenHashTable;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class FFMPredictionModelTest {

    @Test
    public void testSerialize() throws IOException, ClassNotFoundException {
        final int factors = 3;
        final int entrySize = Entry.sizeOf(factors);

        HeapBuffer buf = new HeapBuffer(HeapBuffer.DEFAULT_CHUNK_SIZE);
        Int2LongOpenHashTable map = Int2LongOpenHashTable.newInstance();

        Entry e1 = new Entry(buf, factors, buf.allocate(entrySize));
        e1.setW(1f);
        e1.setV(new float[] {1f, -1f, -1f});

        Entry e2 = new Entry(buf, factors, buf.allocate(entrySize));
        e2.setW(2f);
        e2.setV(new float[] {1f, 2f, -1f});

        Entry e3 = new Entry(buf, factors, buf.allocate(entrySize));
        e3.setW(3f);
        e3.setV(new float[] {1f, 2f, 3f});

        map.put(1, e1.getOffset());
        map.put(2, e2.getOffset());
        map.put(3, e3.getOffset());

        FFMPredictionModel expected = new FFMPredictionModel(map, buf, 0.d, 3,
            Feature.DEFAULT_NUM_FEATURES, Feature.DEFAULT_NUM_FIELDS);
        byte[] b = expected.serialize();

        FFMPredictionModel actual = FFMPredictionModel.deserialize(b, b.length);
        Assert.assertEquals(3, actual.getNumFactors());
        Assert.assertEquals(Feature.DEFAULT_NUM_FEATURES, actual.getNumFeatures());
        Assert.assertEquals(Feature.DEFAULT_NUM_FIELDS, actual.getNumFields());
    }

}
