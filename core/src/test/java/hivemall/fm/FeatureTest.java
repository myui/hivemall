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
package hivemall.fm;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Test;

public class FeatureTest {

    @Test
    public void testParseFeature() throws HiveException {
        Feature f2 = Feature.parseFeature("1164:0.3652", false);
        Assert.assertTrue(f2 instanceof StringFeature);
        Assert.assertEquals("1164", f2.getFeature());
        Assert.assertEquals(0.3652d, f2.getValue(), 0.d);
    }

    @Test
    public void testParseFFMFeature() throws HiveException {
        IntFeature f1 = Feature.parseFFMFeature("2:1163:0.3651");
        Assert.assertEquals(2, f1.getField());
        Assert.assertEquals(1163, f1.getFeatureIndex());
        Assert.assertEquals("1163", f1.getFeature());
        Assert.assertEquals(0.3651d, f1.getValue(), 0.d);
    }

    @Test
    public void testParseQuantitativeFFMFeature() throws HiveException {
        IntFeature f1 = Feature.parseFFMFeature("163:0.3651");
        Assert.assertEquals(163, f1.getField());
        Assert.assertEquals(163, f1.getFeatureIndex());
        Assert.assertEquals("163", f1.getFeature());
        Assert.assertEquals(0.3651d, f1.getValue(), 0.d);
    }

    @Test(expected = HiveException.class)
    public void testParseQuantitativeFFMFeatureFails() throws HiveException {
        Feature.parseFFMFeature("1163:0.3651");
    }

    @Test
    public void testParseFeatureProbe() throws HiveException {
        Feature probe = Feature.parseFeature("dummy:-1", false);
        Feature.parseFeature("1164:0.3652", probe, false);
        Assert.assertEquals("1164", probe.getFeature());
        Assert.assertEquals(0.3652d, probe.getValue(), 0.d);
    }

    public void testParseFFMFeatureProbe() throws HiveException {
        IntFeature probe = Feature.parseFFMFeature("dummyFeature:dummyField:-1");
        Feature.parseFFMFeature("2:1163:0.3651", probe);
        Assert.assertEquals(2, probe.getField());
        Assert.assertEquals(1163, probe.getFeatureIndex());
        Assert.assertEquals("1163", probe.getFeature());
        Assert.assertEquals(0.3651d, probe.getValue(), 0.d);
    }

    public void testParseIntFeature() throws HiveException {
        Feature f = Feature.parseFeature("1163:0.3651", true);
        Assert.assertTrue(f instanceof IntFeature);
        Assert.assertEquals("1163", f.getFeature());
        Assert.assertEquals(1163, f.getFeatureIndex());
        Assert.assertEquals(0.3651d, f.getValue(), 0.d);
    }

    @Test(expected = HiveException.class)
    public void testParseIntFeatureFails() throws HiveException {
        Feature.parseFeature("2:1163:0.3651", true);
    }

}
