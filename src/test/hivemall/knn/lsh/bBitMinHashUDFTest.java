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
package hivemall.knn.lsh;

import hivemall.knn.distance.PopcountUDF;
import hivemall.knn.lsh.bBitMinHashUDF;

import java.math.BigInteger;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

public class bBitMinHashUDFTest {

    @Test
    public void test() throws HiveException {
        bBitMinHashUDF bbminhash = new bBitMinHashUDF();
        PopcountUDF popcnt = new PopcountUDF();

        String a1 = bbminhash.evaluate(Arrays.asList("a", "b", "c"), false);
        String a2 = bbminhash.evaluate(Arrays.asList("a", "b"), false);
        Assert.assertFalse("a1: " + a1, bigint(a1).compareTo(bigint(0L)) < 0);
        Assert.assertFalse("a2: " + a2, bigint(a2).compareTo(bigint(0L)) < 0);
        Assert.assertTrue(popcnt.evaluate(a1, a2) > 0);

        String b1 = bbminhash.evaluate(Arrays.asList("a"), false);
        String b2 = bbminhash.evaluate(Arrays.asList("b", "c"), false);
        Assert.assertFalse("b1: " + b1, bigint(b1).compareTo(bigint(0L)) < 0);
        Assert.assertFalse("b2: " + b2, bigint(b2).compareTo(bigint(0L)) < 0);
        Assert.assertTrue(popcnt.evaluate(a1, a2) > popcnt.evaluate(b1, b2));

        String c1 = bbminhash.evaluate(Arrays.asList("a", "b", "c", "d", "e"), false);
        String c2 = bbminhash.evaluate(Arrays.asList("b", "c", "e", "d"), false);
        Assert.assertTrue(popcnt.evaluate(c1, c2) > popcnt.evaluate(a1, a2));
    }

    private static BigInteger bigint(long l) {
        return BigInteger.valueOf(l);
    }

    private static BigInteger bigint(String s) {
        return new BigInteger(s);
    }

}
