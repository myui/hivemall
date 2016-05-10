/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
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
package hivemall.fm;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

public class FeatureTest {

    @Test
    public void testParseStringBoolean() throws HiveException {
        Feature f1 = Feature.parse("2:1163:0.3651", false);
        Assert.assertTrue(f1 instanceof StringFeature);
        Assert.assertEquals("2", f1.getField());
        Assert.assertEquals("1163", f1.getFeature());
        Assert.assertEquals(0.3651d, f1.getValue(), 0.d);

        Feature f2 = Feature.parse("1164:0.3652", false);
        Assert.assertTrue(f2 instanceof StringFeature);
        Assert.assertEquals("1164", f2.getField());
        Assert.assertEquals("1164", f2.getFeature());
        Assert.assertEquals(0.3652d, f2.getValue(), 0.d);
    }

    @Test
    public void testParseStringFeatureBoolean() throws HiveException {
        Feature probe = new StringFeature("dummyFeature", "dummyField", Double.NaN);
        Feature.parse("2:1163:0.3651", probe, false);
        Assert.assertEquals("2", probe.getField());
        Assert.assertEquals("1163", probe.getFeature());
        Assert.assertEquals(0.3651d, probe.getValue(), 0.d);

        Feature.parse("1164:0.3652", probe, false);
        Assert.assertEquals("1164", probe.getField());
        Assert.assertEquals("1164", probe.getFeature());
        Assert.assertEquals(0.3652d, probe.getValue(), 0.d);
    }

    public void testParseIntFeature() throws HiveException {
        Feature f = Feature.parse("1163:0.3651", true);
        Assert.assertTrue(f instanceof IntFeature);
        Assert.assertEquals("1163", f.getFeature());
        Assert.assertEquals(1163, f.getFeatureIndex());
        Assert.assertEquals(0.3651d, f.getValue(), 0.d);
    }

    @Test(expected = HiveException.class)
    public void testParseIntFeatureFails() throws HiveException {
        Feature.parse("2:1163:0.3651", true);
    }

}
