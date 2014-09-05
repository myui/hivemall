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
package hivemall.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class FeatureValueTest {

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
