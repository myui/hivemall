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

public class Sha1UDFTest {

    @Test
    public void testEvaluate() {
        Sha1UDF udf = new Sha1UDF();

        assertEquals(
            "sha1('hive') == 14489007",
            14489007,
            udf.evaluate("hive")
        );
        assertEquals(
            "sha1('mall') == 8556780",
            8556780,
            udf.evaluate("mall")
        );
    }

    @Test
    public void testEvaluateWithNumFeatures() {
        final int numFeatures = 1 << 24;
        Sha1UDF udf = new Sha1UDF();

        assertEquals(
            "sha1('hive') == 14489007",
            14489007,
            udf.evaluate("hive", numFeatures)
        );
        assertEquals(
            "sha1('mall') == 8556780",
            8556780,
            udf.evaluate("mall", numFeatures)
        );
    }

    @Test
    public void testEvaluateArray() {
        final String[] words = {"hive", "mall"};
        final String[] noWords = {};
        final String[] oneWord = {"hivemall"};
        Sha1UDF udf = new Sha1UDF();

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
    public void testEvaluateArrayWithNumFeatures() {
        final int numFeatures = 1 << 24;
        final String[] words = {"hive", "mall"};
        final String[] noWords = {};
        final String[] oneWord = {"hivemall"};
        Sha1UDF udf = new Sha1UDF();

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
