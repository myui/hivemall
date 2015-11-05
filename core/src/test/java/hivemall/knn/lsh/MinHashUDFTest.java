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
package hivemall.knn.lsh;

import hivemall.knn.lsh.MinHashesUDF;

import java.util.Arrays;


import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;
import org.junit.Assert;

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
