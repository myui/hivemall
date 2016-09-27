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
package hivemall.evaluation;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * Binary responses measures for item recommendation (i.e. ranking problems)
 *
 * References: B. McFee and G. R. Lanckriet. "Metric Learning to Rank" ICML 2010.
 */
public final class BinaryResponsesMeasures {

    private BinaryResponsesMeasures() {}

    /**
     * Computes binary nDCG (i.e. relevance score is 0 or 1)
     *
     * @param rankedList a list of ranked item IDs (first item is highest-ranked)
     * @param groundTruth a collection of positive/correct item IDs
     * @param recommendSize top-`recommendSize` items in `rankedList` are recommended
     * @return nDCG
     */
    public static double nDCG(@Nonnull final List<?> rankedList,
            @Nonnull final List<?> groundTruth, @Nonnull final int recommendSize) {
        double dcg = 0.d;
        double idcg = IDCG(Math.min(recommendSize, groundTruth.size()));

        for (int i = 0, n = recommendSize; i < n; i++) {
            Object item_id = rankedList.get(i);
            if (!groundTruth.contains(item_id)) {
                continue;
            }
            int rank = i + 1;
            dcg += Math.log(2) / Math.log(rank + 1);
        }

        return dcg / idcg;
    }

    /**
     * Computes the ideal DCG
     * 
     * @param n the number of positive items
     * @return ideal DCG
     */
    public static double IDCG(final int n) {
        double idcg = 0.d;
        for (int i = 0; i < n; i++) {
            idcg += Math.log(2) / Math.log(i + 2);
        }
        return idcg;
    }

    /**
     * Computes Precision@`recommendSize`
     *
     * @param rankedList a list of ranked item IDs (first item is highest-ranked)
     * @param groundTruth a collection of positive/correct item IDs
     * @param recommendSize top-`recommendSize` items in `rankedList` are recommended
     * @return Precision
     */
    public static double Precision(@Nonnull final List<?> rankedList,
            @Nonnull final List<?> groundTruth, @Nonnull final int recommendSize) {
        return (double) countTruePositive(rankedList, groundTruth, recommendSize) / recommendSize;
    }

    /**
     * Computes Recall@`recommendSize`
     *
     * @param rankedList a list of ranked item IDs (first item is highest-ranked)
     * @param groundTruth a collection of positive/correct item IDs
     * @param recommendSize top-`recommendSize` items in `rankedList` are recommended
     * @return Recall
     */
    public static double Recall(@Nonnull final List<?> rankedList,
            @Nonnull final List<?> groundTruth, @Nonnull final int recommendSize) {
        return (double) countTruePositive(rankedList, groundTruth, recommendSize)
                / groundTruth.size();
    }

    /**
     * Counts the number of true positives
     *
     * @param rankedList a list of ranked item IDs (first item is highest-ranked)
     * @param groundTruth a collection of positive/correct item IDs
     * @param recommendSize top-`recommendSize` items in `rankedList` are recommended
     * @return number of true positives
     */
    public static int countTruePositive(final List<?> rankedList, final List<?> groundTruth,
            final int recommendSize) {
        int nTruePositive = 0;

        for (int i = 0, n = recommendSize; i < n; i++) {
            Object item_id = rankedList.get(i);
            if (groundTruth.contains(item_id)) {
                nTruePositive++;
            }
        }

        return nTruePositive;
    }

    /**
     * Computes Mean Reciprocal Rank (MRR)
     *
     * @param rankedList a list of ranked item IDs (first item is highest-ranked)
     * @param groundTruth a collection of positive/correct item IDs
     * @param recommendSize top-`recommendSize` items in `rankedList` are recommended
     * @return MRR
     */
    public static double MRR(@Nonnull final List<?> rankedList, @Nonnull final List<?> groundTruth,
            @Nonnull final int recommendSize) {
        for (int i = 0, n = recommendSize; i < n; i++) {
            Object item_id = rankedList.get(i);
            if (groundTruth.contains(item_id)) {
                return 1.0 / (i + 1.0);
            }
        }

        return 0.0;
    }

    /**
     * Computes Mean Average Precision (MAP)
     *
     * @param rankedList a list of ranked item IDs (first item is highest-ranked)
     * @param groundTruth a collection of positive/correct item IDs
     * @param recommendSize top-`recommendSize` items in `rankedList` are recommended
     * @return MAP
     */
    public static double MAP(@Nonnull final List<?> rankedList, @Nonnull final List<?> groundTruth,
            @Nonnull final int recommendSize) {
        int nTruePositive = 0;
        double sumPrecision = 0.0;

        // accumulate precision@1 to @recommendSize
        for (int i = 0, n = recommendSize; i < n; i++) {
            Object item_id = rankedList.get(i);
            if (groundTruth.contains(item_id)) {
                nTruePositive++;
                sumPrecision += nTruePositive / (i + 1.0);
            }
        }

        return sumPrecision / groundTruth.size();
    }

    /**
     * Computes the area under the ROC curve (AUC)
     *
     * @param rankedList a list of ranked item IDs (first item is highest-ranked)
     * @param groundTruth a collection of positive/correct item IDs
     * @param recommendSize top-`recommendSize` items in `rankedList` are recommended
     * @return AUC
     */
    public static double AUC(@Nonnull final List<?> rankedList, @Nonnull final List<?> groundTruth,
            @Nonnull final int recommendSize) {
        int nTruePositive = 0, nCorrectPairs = 0;

        // count # of pairs of items that are ranked in the correct order (i.e. TP > FP)
        for (int i = 0, n = recommendSize; i < n; i++) {
            Object item_id = rankedList.get(i);
            if (groundTruth.contains(item_id)) {
                // # of true positives which are ranked higher position than i-th recommended item
                nTruePositive++;
            } else {
                // for each FP item, # of correct ordered <TP, FP> pairs equals to # of TPs at i-th position
                nCorrectPairs += nTruePositive;
            }
        }

        // # of all possible <TP, FP> pairs
        int nPairs = nTruePositive * (recommendSize - nTruePositive);

        // AUC can equivalently be calculated by counting the portion of correctly ordered pairs
        return (double) nCorrectPairs / nPairs;
    }

}
