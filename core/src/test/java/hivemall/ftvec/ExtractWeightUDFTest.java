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
package hivemall.ftvec;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.Assert;
import org.junit.Test;

public class ExtractWeightUDFTest {

    @Test
    public void testExtractWeights() throws UDFArgumentException {
        Assert.assertEquals(1.1d, ExtractWeightUDF.extractWeights("aaa:bbb:1.1d").get(), 0.d);
        Assert.assertEquals(1.1d, ExtractWeightUDF.extractWeights("aaa:1.1d").get(), 0.d);
        Assert.assertEquals(1.d, ExtractWeightUDF.extractWeights("bbb").get(), 0.d);
        Assert.assertEquals(1.d, ExtractWeightUDF.extractWeights("222").get(), 0.d);
    }

    @Test(expected = UDFArgumentException.class)
    public void testFail() throws UDFArgumentException {
        ExtractWeightUDF.extractWeights("aaa:");
    }

}
