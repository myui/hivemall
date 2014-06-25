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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.Test;

public class MurmurHash3UDFTest {

    @Test
    public void testEvaluate() throws UDFArgumentException {
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals("hash('hive') == 1966096", 1966096, udf.evaluate("hive").get());
        assertEquals("hash('mall') == 36970", 36970, udf.evaluate("mall").get());
    }

    @Test
    public void testEvaluateWithNumFeatures() throws UDFArgumentException {
        /* '1 << 24' (16777216) is default number of features */
        final int numFeatures = 1 << 24;
        final int smallNumFeatures = 10;
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals("hash('hive') == 1966096", 1966096, udf.evaluate("hive", numFeatures).get());
        assertEquals("hash('mall') == 36970", 36970, udf.evaluate("mall", numFeatures).get());

        assertEquals("hash('hive') == 8", 8, udf.evaluate("hive", smallNumFeatures).get());
        assertEquals("hash('mall') == 0", 0, udf.evaluate("mall", smallNumFeatures).get());
    }

    @Test
    public void testEvaluateArray() throws UDFArgumentException {
        final String[] words = { "hive", "mall" };
        final String[] noWords = {};
        final String[] oneWord = { "hivemall" };
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(udf.evaluate("hive\tmall"), udf.evaluate(Arrays.asList(words)));
        assertEquals(0, udf.evaluate(Arrays.asList(noWords)).get());
        assertEquals(udf.evaluate("hivemall"), udf.evaluate(Arrays.asList(oneWord)));
    }

    @Test
    public void testEvaluateArrayWithNumFeatures() throws UDFArgumentException {
        final int numFeatures = 1 << 24;
        final String[] words = { "hive", "mall" };
        final String[] noWords = {};
        final String[] oneWord = { "hivemall" };
        MurmurHash3UDF udf = new MurmurHash3UDF();

        assertEquals(udf.evaluate("hive\tmall", numFeatures), udf.evaluate(Arrays.asList(words), numFeatures));
        assertEquals(0, udf.evaluate(Arrays.asList(noWords), numFeatures).get());
        assertEquals(udf.evaluate("hivemall", numFeatures), udf.evaluate(Arrays.asList(oneWord), numFeatures));
    }

}
