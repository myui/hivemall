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

public class PoweredFeaturesUDFTest {

    @Test(expected = HiveException.class)
    public void testIllegalDegree() throws HiveException {
        List<Text> args = WritableUtils.val("a:0.5", "b:0.3");
        PoweredFeaturesUDF udf = new PoweredFeaturesUDF();
        udf.evaluate(args, 1);
    }

    @Test
    public void testAB() throws HiveException {
        List<Text> args = WritableUtils.val("a:0.5", "b:0.3");
        PoweredFeaturesUDF udf = new PoweredFeaturesUDF();
        List<Text> actuals = udf.evaluate(args, 2);
        List<Text> expected = WritableUtils.val("a:0.5", "a^2:0.25", "b:0.3", "b^2:0.09");
        //System.out.println(actuals);
        Assert.assertEquals(expected, actuals);

        actuals = udf.evaluate(args, 3);
        expected = WritableUtils.val("a:0.5", "a^2:0.25", "a^3:0.125", "b:0.3", "b^2:0.09",
            "b^3:0.027000003");
        //System.out.println(actuals);
        Assert.assertEquals(expected, actuals);
    }

}
