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
package hivemall.ftvec.pairing;

import hivemall.utils.hadoop.WritableUtils;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class PolynomialFeaturesUDFTest {

    @Test(expected = HiveException.class)
    public void testIllegalDegree() throws HiveException {
        List<Text> args = WritableUtils.val("a:0.5", "b:0.3");
        PolynomialFeaturesUDF udf = new PolynomialFeaturesUDF();
        udf.evaluate(args, 1);
    }

    @Test
    public void testABC() throws HiveException {
        List<Text> args = WritableUtils.val("a:0.5", "b:0.3");
        PolynomialFeaturesUDF udf = new PolynomialFeaturesUDF();
        List<Text> actuals = udf.evaluate(args, 2);
        List<Text> expected = WritableUtils.val("a:0.5", "a^a:0.25", "a^b:0.15", "b:0.3",
            "b^b:0.09");
        Assert.assertEquals(expected, actuals);

        actuals = udf.evaluate(args, 3);
        expected = WritableUtils.val("a:0.5", "a^a:0.25", "a^a^a:0.125", "a^a^b:0.075", "a^b:0.15",
            "a^b^b:0.045", "b:0.3", "b^b:0.09", "b^b^b:0.027000003");
        Assert.assertEquals(expected, actuals);

        args = WritableUtils.val("a:0.5", "b:0.3", "c:0.2");
        actuals = udf.evaluate(args, 3);
        expected = WritableUtils.val("a:0.5", "a^a:0.25", "a^a^a:0.125", "a^a^b:0.075",
            "a^a^c:0.05", "a^b:0.15", "a^b^b:0.045", "a^b^c:0.030000001", "a^c:0.1",
            "a^c^c:0.020000001", "b:0.3", "b^b:0.09", "b^b^b:0.027000003", "b^b^c:0.018000001",
            "b^c:0.060000002", "b^c^c:0.012000001", "c:0.2", "c^c:0.040000003", "c^c^c:0.008");
        Assert.assertEquals(expected, actuals);
    }

    @Test
    public void testTruncate() throws HiveException {
        List<Text> args = WritableUtils.val("a:0.5", "b:1.0", "c:0.2");
        PolynomialFeaturesUDF udf = new PolynomialFeaturesUDF();
        List<Text> actuals = udf.evaluate(args, 3);
        List<Text> expected = WritableUtils.val("a:0.5", "a^a:0.25", "a^a^a:0.125", "a^a^c:0.05",
            "a^c:0.1", "a^c^c:0.020000001", "b:1.0", "c:0.2", "c^c:0.040000003", "c^c^c:0.008");
        Assert.assertEquals(expected, actuals);

        actuals = udf.evaluate(args, 3, false, false);
        expected = WritableUtils.val("a:0.5", "a^a:0.25", "a^a^a:0.125", "a^a^b:0.25",
            "a^a^c:0.05", "a^b:0.5", "a^b^b:0.5", "a^b^c:0.1", "a^c:0.1", "a^c^c:0.020000001",
            "b:1.0", "b^b:1.0", "b^b^b:1.0", "b^b^c:0.2", "b^c:0.2", "b^c^c:0.040000003", "c:0.2",
            "c^c:0.040000003", "c^c^c:0.008");
        Assert.assertEquals(expected, actuals);
    }

    @Test
    public void testInteractionOnly() throws HiveException {
        List<Text> args = WritableUtils.val("a:0.5", "b:0.3", "c:0.2");
        PolynomialFeaturesUDF udf = new PolynomialFeaturesUDF();
        List<Text> actuals = udf.evaluate(args, 3, true, true);
        List<Text> expected = WritableUtils.val("a:0.5", "a^b:0.15", "a^b^c:0.030000001",
            "a^c:0.1", "b:0.3", "b^c:0.060000002", "c:0.2");
        Assert.assertEquals(expected, actuals);
    }

}
