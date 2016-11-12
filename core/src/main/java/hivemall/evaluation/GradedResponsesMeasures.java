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

import java.util.List;

import javax.annotation.Nonnull;

/**
 * Utility class of various measures.
 * 
 * See http://recsyswiki.com/wiki/Discounted_Cumulative_Gain
 */
public final class GradedResponsesMeasures {

    private GradedResponsesMeasures() {}

    public static double nDCG(@Nonnull final List<Double> recommendTopRelScoreList,
            @Nonnull final List<Double> truthTopRelScoreList, @Nonnull final int recommendSize) {
        double dcg = DCG(recommendTopRelScoreList, recommendSize);
        double idcg = DCG(truthTopRelScoreList, recommendSize);
        return dcg / idcg;
    }

    /**
     * Computes DCG
     * 
     * @param topRelScoreList ranked list of top relevance scores
     * @param recommendSize the number of positive items
     * @return DCG
     */
    public static double DCG(final List<Double> topRelScoreList, final int recommendSize) {
        double dcg = 0.d;
        for (int i = 0; i < recommendSize; i++) {
            double relScore = topRelScoreList.get(i);
            dcg += ((Math.pow(2, relScore) - 1) * Math.log(2)) / Math.log(i + 2);
        }
        return dcg;
    }

}
