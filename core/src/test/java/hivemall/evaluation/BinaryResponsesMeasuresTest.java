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

import java.util.Collections;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class BinaryResponsesMeasuresTest {

    @Test
    public void testNDCG() {
        List<Integer> rankedList = Arrays.asList(1, 3, 2, 6);
        List<Integer> groundTruth = Arrays.asList(1, 2, 4);

        double actual = BinaryResponsesMeasures.nDCG(rankedList, groundTruth, rankedList.size());
        Assert.assertEquals(0.7039180890341348d, actual, 0.0001d);

        actual = BinaryResponsesMeasures.nDCG(rankedList, groundTruth, 2);
        Assert.assertEquals(0.6131471927654585d, actual, 0.0001d);
    }

    @Test
    public void testRecall() {
        List<Integer> rankedList = Arrays.asList(1, 3, 2, 6);
        List<Integer> groundTruth = Arrays.asList(1, 2, 4);

        double actual = BinaryResponsesMeasures.Recall(rankedList, groundTruth, rankedList.size());
        Assert.assertEquals(0.6666666666666666d, actual, 0.0001d);

        actual = BinaryResponsesMeasures.Recall(rankedList, groundTruth, 2);
        Assert.assertEquals(0.3333333333333333d, actual, 0.0001d);
    }

    @Test
    public void testPrecision() {
        List<Integer> rankedList = Arrays.asList(1, 3, 2, 6);
        List<Integer> groundTruth = Arrays.asList(1, 2, 4);

        double actual = BinaryResponsesMeasures.Precision(rankedList, groundTruth, rankedList.size());
        Assert.assertEquals(0.5d, actual, 0.0001d);

        actual = BinaryResponsesMeasures.Precision(rankedList, groundTruth, 2);
        Assert.assertEquals(0.5d, actual, 0.0001d);
    }

    @Test
    public void testMRR() {
        List<Integer> rankedList = Arrays.asList(1, 3, 2, 6);
        List<Integer> groundTruth = Arrays.asList(1, 2, 4);

        double actual = BinaryResponsesMeasures.MRR(rankedList, groundTruth, rankedList.size());
        Assert.assertEquals(1.0d, actual, 0.0001d);

        Collections.reverse(rankedList);

        actual = BinaryResponsesMeasures.MRR(rankedList, groundTruth, rankedList.size());
        Assert.assertEquals(0.5d, actual, 0.0001d);

        actual = BinaryResponsesMeasures.MRR(rankedList, groundTruth, 1);
        Assert.assertEquals(0.0d, actual, 0.0001d);
    }

    @Test
    public void testMAP() {
        List<Integer> rankedList = Arrays.asList(1, 3, 2, 6);
        List<Integer> groundTruth = Arrays.asList(1, 2, 4);

        double actual = BinaryResponsesMeasures.MAP(rankedList, groundTruth, rankedList.size());
        Assert.assertEquals(0.5555555555555555d, actual, 0.0001d);

        actual = BinaryResponsesMeasures.MAP(rankedList, groundTruth, 2);
        Assert.assertEquals(0.3333333333333333d, actual, 0.0001d);
    }

    @Test
    public void testAUC() {
        List<Integer> rankedList = Arrays.asList(1, 3, 2, 6);
        List<Integer> groundTruth = Arrays.asList(1, 2, 4);

        double actual = BinaryResponsesMeasures.AUC(rankedList, groundTruth, rankedList.size());
        Assert.assertEquals(0.75d, actual, 0.0001d);

        actual = BinaryResponsesMeasures.AUC(rankedList, groundTruth, 2);
        Assert.assertEquals(1.0d, actual, 0.0001d);
    }

}
