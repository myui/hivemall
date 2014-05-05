/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
