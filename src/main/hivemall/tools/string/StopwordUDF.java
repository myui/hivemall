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
package hivemall.tools.string;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * This class uses Stopwords Corpus obtained from PostgreSQL.
 * http://anoncvs.postgresql.org/cvsweb.cgi/pgsql/src/backend/snowball/stopwords/
 */
public class StopwordUDF extends UDF {

    private static final String[] stopwords;
    static {
        stopwords = new String[] { "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
                "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself",
                "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their",
                "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these",
                "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has",
                "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but",
                "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
                "about", "against", "between", "into", "through", "during", "before", "after",
                "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over",
                "under", "again", "further", "then", "once", "here", "there", "when", "where",
                "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some",
                "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very",
                "s", "t", "can", "will", "just", "don", "should", "now" };
        Arrays.sort(stopwords);
    }

    public boolean evaluate(String word) {
        return Arrays.binarySearch(stopwords, word) != -1;
    }

}
