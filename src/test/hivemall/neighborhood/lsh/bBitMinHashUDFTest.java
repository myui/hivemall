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

import hivemall.neighborhood.distance.PopcountUDF;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

public class bBitMinHashUDFTest {

    @Test
    public void test() throws HiveException {
        bBitMinHashUDF bbminhash = new bBitMinHashUDF();
        PopcountUDF popcnt = new PopcountUDF();

        Long a1 = bbminhash.evaluate(Arrays.asList("a", "b", "c"), false);
        Long a2 = bbminhash.evaluate(Arrays.asList("a", "b"), false);
        Assert.assertTrue(popcnt.evaluate(a1, a2) > 0);

        Long b1 = bbminhash.evaluate(Arrays.asList("a"), false);
        Long b2 = bbminhash.evaluate(Arrays.asList("b", "c"), false);
        Assert.assertTrue(popcnt.evaluate(a1, a2) > popcnt.evaluate(b1, b2));

        Long c1 = bbminhash.evaluate(Arrays.asList("a", "b", "c", "d", "e"), false);
        Long c2 = bbminhash.evaluate(Arrays.asList("b", "c", "e", "d"), false);
        Assert.assertTrue(popcnt.evaluate(c1, c2) > popcnt.evaluate(a1, a2));
    }

}
