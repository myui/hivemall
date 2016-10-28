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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class Sha1UDFTest {

    @Test
    public void testEvaluate() {
        Sha1UDF udf = new Sha1UDF();

        assertEquals("sha1('hive') == 14489008", 14489008, udf.evaluate("hive").get());
        assertEquals("sha1('mall') == 8556781", 8556781, udf.evaluate("mall").get());
    }

    @Test
    public void testEvaluateWithNumFeatures() {
        final int numFeatures = 1 << 24;
        Sha1UDF udf = new Sha1UDF();

        assertEquals("sha1('hive') == 14489008", 14489008, udf.evaluate("hive", numFeatures).get());
        assertEquals("sha1('mall') == 8556781", 8556781, udf.evaluate("mall", numFeatures).get());
    }

    @Test
    public void testEvaluateArray() {
        final List<String> words = Arrays.asList("hive", "mall");
        final List<String> noWords = Collections.emptyList();
        final List<String> oneWord = Arrays.asList("hivemall");
        Sha1UDF udf = new Sha1UDF();

        assertEquals(udf.evaluate("hive\tmall"), udf.evaluate(words));
        assertEquals(1, udf.evaluate(noWords).get());
        assertEquals(udf.evaluate("hivemall"), udf.evaluate(oneWord));
    }

    @Test
    public void testEvaluateArrayWithNumFeatures() {
        final int numFeatures = 1 << 24;
        final List<String> words = Arrays.asList("hive", "mall");
        final List<String> noWords = Collections.emptyList();
        final List<String> oneWord = Arrays.asList("hivemall");
        Sha1UDF udf = new Sha1UDF();

        assertEquals(udf.evaluate("hive\tmall", numFeatures), udf.evaluate(words, numFeatures));
        assertEquals(1, udf.evaluate(noWords, numFeatures).get());
        assertEquals(udf.evaluate("hivemall", numFeatures), udf.evaluate(oneWord, numFeatures));
    }
}
