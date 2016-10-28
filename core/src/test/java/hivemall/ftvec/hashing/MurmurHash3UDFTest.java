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
package hivemall.ftvec.hashing;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.Test;

public class MurmurHash3UDFTest {

    @Test
    public void testEvaluate() throws UDFArgumentException {
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals("hash('hive') == 1966097", 1966097, udf.evaluate("hive").get());
        assertEquals("hash('mall') == 36971", 36971, udf.evaluate("mall").get());
    }

    @Test
    public void testEvaluateWithNumFeatures() throws UDFArgumentException {
        /* '1 << 24' (16777216) is default number of features */
        final int numFeatures = 1 << 24;
        final int smallNumFeatures = 10;
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals("hash('hive') == 1966097", 1966097, udf.evaluate("hive", numFeatures).get());
        assertEquals("hash('mall') == 36971", 36971, udf.evaluate("mall", numFeatures).get());

        assertEquals("hash('hive') == 9", 9, udf.evaluate("hive", smallNumFeatures).get());
        assertEquals("hash('mall') == 1", 1, udf.evaluate("mall", smallNumFeatures).get());
    }

    @Test
    public void testEvaluateArray() throws UDFArgumentException {
        final String[] words = {"hive", "mall"};
        final String[] noWords = {};
        final String[] oneWord = {"hivemall"};
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(udf.evaluate("hive\tmall"), udf.evaluate(Arrays.asList(words)));
        assertEquals(1, udf.evaluate(Arrays.asList(noWords)).get());
        assertEquals(udf.evaluate("hivemall"), udf.evaluate(Arrays.asList(oneWord)));
    }

    @Test
    public void testEvaluateArrayWithNumFeatures() throws UDFArgumentException {
        final int numFeatures = 1 << 24;
        final String[] words = {"hive", "mall"};
        final String[] noWords = {};
        final String[] oneWord = {"hivemall"};
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(udf.evaluate("hive\tmall", numFeatures),
            udf.evaluate(Arrays.asList(words), numFeatures));
        assertEquals(1, udf.evaluate(Arrays.asList(noWords), numFeatures).get());
        assertEquals(udf.evaluate("hivemall", numFeatures),
            udf.evaluate(Arrays.asList(oneWord), numFeatures));
    }

}
