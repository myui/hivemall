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
package hivemall.evaluation;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GradedResponsesMeasuresTest {

    @Test
    public void testNDCG() {
        List<Double> recommendTopRelScoreList = Arrays.asList(5.0, 2.0, 4.0, 1.0, 3.0);
        List<Double> truthTopRelScoreList = Arrays.asList(5.0, 4.0, 3.0);

        double actual = GradedResponsesMeasures.nDCG(recommendTopRelScoreList, truthTopRelScoreList, 3);
        Assert.assertEquals(0.918770780535d, actual, 0.0001d);

        actual = GradedResponsesMeasures.nDCG(recommendTopRelScoreList, truthTopRelScoreList, 2);
        Assert.assertEquals(0.812891283859d, actual, 0.0001d);
    }

}
