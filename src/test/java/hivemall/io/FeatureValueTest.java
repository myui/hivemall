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
package hivemall.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import hivemall.test.HivemallTestBase;
import org.junit.Test;

public class FeatureValueTest extends HivemallTestBase {

    @Test
    public void testParseWithoutWeight() {
        String expectedFeature = "ad_url|891572";
        FeatureValue fv = FeatureValue.parse(expectedFeature);
        assertNotNull(fv);
        assertEquals(expectedFeature, fv.getFeature().toString());
        assertEquals(1.f, fv.getValue(), 0.f);

        expectedFeature = "891572";
        fv = FeatureValue.parse(expectedFeature);
        assertNotNull(fv);
        assertEquals(expectedFeature, fv.getFeature().toString());
        assertEquals(1.f, fv.getValue(), 0.f);
    }

    @Test
    public void testParseWithWeight() {
        String expectedFeature = "ad_url:0.5";
        FeatureValue fv = FeatureValue.parse(expectedFeature);
        assertNotNull(fv);
        assertEquals("ad_url", fv.getFeature().toString());
        assertEquals(0.5f, fv.getValue(), 0.f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseExpectingIllegalArgumentException() {
        FeatureValue.parse("ad_url:");
    }

    @Test(expected = NumberFormatException.class)
    public void testParseExpectingNumberFormatException() {
        FeatureValue.parse("ad_url:xxxxx");
    }

}
