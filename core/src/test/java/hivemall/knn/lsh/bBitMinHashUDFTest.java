/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.knn.lsh;

import hivemall.knn.distance.PopcountUDF;

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

        String a1 = bbminhash.evaluate(Arrays.asList("a", "b", "c"), false).toString();
        String a2 = bbminhash.evaluate(Arrays.asList("a", "b"), false).toString();
        Assert.assertFalse("a1: " + a1, bigint(a1).compareTo(bigint(0L)) < 0);
        Assert.assertFalse("a2: " + a2, bigint(a2).compareTo(bigint(0L)) < 0);
        Assert.assertTrue(popcnt.evaluate(a1, a2).get() > 0);

        String b1 = bbminhash.evaluate(Arrays.asList("a"), false).toString();
        String b2 = bbminhash.evaluate(Arrays.asList("b", "c"), false).toString();
        Assert.assertFalse("b1: " + b1, bigint(b1).compareTo(bigint(0L)) < 0);
        Assert.assertFalse("b2: " + b2, bigint(b2).compareTo(bigint(0L)) < 0);
        Assert.assertTrue(popcnt.evaluate(a1, a2).get() > popcnt.evaluate(b1, b2).get());

        String c1 = bbminhash.evaluate(Arrays.asList("a", "b", "c", "d", "e"), false).toString();
        String c2 = bbminhash.evaluate(Arrays.asList("b", "c", "e", "d"), false).toString();
        Assert.assertTrue(popcnt.evaluate(c1, c2).get() > popcnt.evaluate(a1, a2).get());
    }

    private static BigInteger bigint(long l) {
        return BigInteger.valueOf(l);
    }

    private static BigInteger bigint(String s) {
        return new BigInteger(s);
    }

}
