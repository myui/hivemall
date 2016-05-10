/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
package hivemall.model;

import hivemall.utils.collections.IMapIterator;

import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class SpaceEfficientDenseModelTest {

    @Test
    public void testGetSet() {
        final int size = 1 << 12;

        final SpaceEfficientDenseModel model1 = new SpaceEfficientDenseModel(size);
        //model1.configureClock();
        final DenseModel model2 = new DenseModel(size);
        //model2.configureClock();

        final Random rand = new Random();
        for(int t = 0; t < 1000; t++) {
            int i = rand.nextInt(size);
            float f = 65520f * rand.nextFloat();
            IWeightValue w = new WeightValue(f);
            model1.set(i, w);
            model2.set(i, w);
        }

        assertEquals(model2.size(), model1.size());

        IMapIterator<Integer, IWeightValue> itor = model1.entries();
        while(itor.next() != -1) {
            int k = itor.getKey();
            float expected = itor.getValue().get();
            float actual = model2.getWeight(k);
            assertEquals(expected, actual, 32f);
        }
    }

}
