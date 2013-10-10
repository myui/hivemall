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
package hivemall.ftvec.hashing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

public class MurmurHash3UDFTest {

    @Test
    public void testEvaluate() throws UDFArgumentException {
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(
            "hash('hive') == 1966096",
            1966096,
            udf.evaluate("hive")
        );
        assertEquals(
            "hash('mall') == 36970",
            36970,
            udf.evaluate("mall")
        );
    }

    @Test
    public void testEvaluateWithNumFeatures() throws UDFArgumentException {
        /* '1 << 24' (16777216) is default number of features */
        final int numFeatures = 1 << 24;
        final int smallNumFeatures = 10;
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(
            "hash('hive') == 1966096",
            1966096,
            udf.evaluate("hive", numFeatures)
        );
        assertEquals(
            "hash('mall') == 36970",
            36970,
            udf.evaluate("mall", numFeatures)
        );

        assertEquals(
            "hash('hive') == 8",
            8,
            udf.evaluate("hive", smallNumFeatures)
        );
        assertEquals(
            "hash('mall') == 0",
            0,
            udf.evaluate("mall", smallNumFeatures)
        );
    }

    @Test
    public void testEvaluateArray() throws UDFArgumentException {
        final String[] words = {"hive", "mall"};
        final String[] noWords = {};
        final String[] oneWord = {"hivemall"};
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(
            udf.evaluate("hive\tmall"),
            udf.evaluate(words)
        );
        assertEquals(
            0,
            udf.evaluate(noWords)
        );
        assertEquals(
            udf.evaluate("hivemall"),
            udf.evaluate(oneWord)
        );
    }

    @Test
    public void testEvaluateArrayWithNumFeatures() throws UDFArgumentException {
        final int numFeatures = 1 << 24;
        final String[] words = {"hive", "mall"};
        final String[] noWords = {};
        final String[] oneWord = {"hivemall"};
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(
            udf.evaluate("hive\tmall", numFeatures),
            udf.evaluate(words, numFeatures)
        );
        assertEquals(
            0,
            udf.evaluate(noWords, numFeatures)
        );
        assertEquals(
            udf.evaluate("hivemall", numFeatures),
            udf.evaluate(oneWord, numFeatures)
        );
    }

}
