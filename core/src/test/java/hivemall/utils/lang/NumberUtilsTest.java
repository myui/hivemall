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
package hivemall.utils.lang;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class NumberUtilsTest {

    @Test
    public void testParseInt() {
        String s1 = "100";
        String s2 = "1k";
        String s3 = "2k";
        String s4 = "1m";
        String s5 = "1g";
        String s6 = "2g";
        String s7 = "2K";
        assertEquals(100, NumberUtils.parseInt(s1));
        assertEquals(1000, NumberUtils.parseInt(s2));
        assertEquals(2000, NumberUtils.parseInt(s3));
        assertEquals(1000000, NumberUtils.parseInt(s4));
        assertEquals(1000000000, NumberUtils.parseInt(s5));
        assertEquals(2000000000, NumberUtils.parseInt(s6));
        assertEquals(2000, NumberUtils.parseInt(s7));
    }

    @Test
    public void testIsFiniteDouble() {
        assertTrue(NumberUtils.isFinite(Double.MAX_VALUE));
        assertTrue(NumberUtils.isFinite(Double.MAX_VALUE - 1.d));
        assertTrue(NumberUtils.isFinite(Double.MIN_VALUE));
        assertTrue(NumberUtils.isFinite(Double.MIN_VALUE + 1.d));
        assertTrue(NumberUtils.isFinite(0.d));
        assertFalse(NumberUtils.isFinite(Double.NaN));
        assertFalse(NumberUtils.isFinite(Double.NEGATIVE_INFINITY));
        assertFalse(NumberUtils.isFinite(Double.POSITIVE_INFINITY));
    }

    @Test
    public void testIsFiniteFloat() {
        assertTrue(NumberUtils.isFinite(Float.MAX_VALUE));
        assertTrue(NumberUtils.isFinite(Float.MAX_VALUE - 1.f));
        assertTrue(NumberUtils.isFinite(Float.MIN_VALUE));
        assertTrue(NumberUtils.isFinite(Float.MIN_VALUE + 1.f));
        assertTrue(NumberUtils.isFinite(0.f));
        assertFalse(NumberUtils.isFinite(Float.NaN));
        assertFalse(NumberUtils.isFinite(Float.NEGATIVE_INFINITY));
        assertFalse(NumberUtils.isFinite(Float.POSITIVE_INFINITY));
    }

}
