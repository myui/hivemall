/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.knn.distance;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class CosineSimilarityUDFTest {

    @Test
    public void testEvaluate() {
        CosineSimilarityUDF cosine = new CosineSimilarityUDF();

        {
            List<String> ftvec1 = Arrays.asList("bbb:1.4", "aaa:0.9", "ccc");
            Assert.assertEquals(1.f, cosine.evaluate(ftvec1, ftvec1, false).get());
        }

        Assert.assertEquals(0.f, cosine.evaluate(Arrays.asList("a", "b", "c"), Arrays.asList("d", "e"), true).get());
        Assert.assertEquals(0.f, cosine.evaluate(Arrays.asList("a", "b", "c"), Arrays.asList("d", "e"), false).get());

        Assert.assertEquals(1.f, cosine.evaluate(Arrays.asList("a", "b"), Arrays.asList("a", "b"), true).get());

        Assert.assertEquals(0.5f, cosine.evaluate(Arrays.asList("a", "b"), Arrays.asList("a", "c"), true).get());

        Assert.assertEquals(-1.f, cosine.evaluate(Arrays.asList("a:1.0"), Arrays.asList("a:-1.0"), false).get());

        Assert.assertTrue(cosine.evaluate(Arrays.asList("apple", "orange"), Arrays.asList("banana", "apple"), true).get() > 0.f);
        Assert.assertTrue(cosine.evaluate(Arrays.asList("apple", "orange"), Arrays.asList("banana", "apple"), false).get() > 0.f);

        Assert.assertTrue((cosine.evaluate(Arrays.asList("apple", "orange"), Arrays.asList("banana", "orange", "apple"), true)).get() > (cosine.evaluate(Arrays.asList("apple", "orange"), Arrays.asList("banana", "orange"), true)).get());

        Assert.assertEquals(1.0f, cosine.evaluate(Arrays.asList("This is a sentence with seven tokens".split(" ")), Arrays.<String> asList("This is a sentence with seven tokens".split(" ")), true).get());
        Assert.assertEquals(1.0f, cosine.evaluate(Arrays.asList("This is a sentence with seven tokens".split(" ")), Arrays.<String> asList("This is a sentence with seven tokens".split(" ")), false).get());

        {
            List<String> tokens1 = Arrays.asList("1:1,2:1,3:1,4:1,5:0,6:1,7:1,8:1,9:0,10:1,11:1".split(","));
            List<String> tokens2 = Arrays.asList("1:1,2:1,3:0,4:1,5:1,6:1,7:1,8:0,9:1,10:1,11:1".split(","));
            Assert.assertEquals(0.77777f, cosine.evaluate(tokens1, tokens2, false).get(), 0.00001f);
        }

        {
            List<String> tokens1 = Arrays.asList("1 2 3 4   6 7 8   10 11".split("\\s+"));
            List<String> tokens2 = Arrays.asList("1 2   4 5 6 7   9 10 11".split("\\s+"));
            double dotp = 1 + 1 + 0 + 1 + 0 + 1 + 1 + 0 + 0 + 1 + 1;
            double norm = Math.sqrt(tokens1.size()) * Math.sqrt(tokens2.size());
            Assert.assertEquals(dotp / norm, cosine.evaluate(tokens1, tokens2, true).get(), 0.00001f);
            Assert.assertEquals(dotp / norm, cosine.evaluate(tokens1, tokens2, false).get(), 0.00001f);

            Assert.assertEquals(dotp / norm, cosine.evaluate(Arrays.asList(1, 2, 3, 4, 6, 7, 8, 10, 11), Arrays.asList(1, 2, 4, 5, 6, 7, 9, 10, 11)).get(), 0.00001f);
        }

        Assert.assertEquals(0.f, cosine.evaluate(Arrays.asList(1, 2, 3), Arrays.asList(4, 5)).get());
        Assert.assertEquals(1.f, cosine.evaluate(Arrays.asList(1, 2), Arrays.asList(1, 2)).get());
    }
}
