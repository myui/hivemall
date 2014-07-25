/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
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
package hivemall.ftvec.scaling;

import static org.junit.Assert.assertEquals;
import hivemall.utils.hadoop.WritableUtils;
import hivemall.utils.math.MathUtils;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class L2NormalizationUDFTest {

    @Test
    public void test() {
        L2NormalizationUDF udf = new L2NormalizationUDF();

        assertEquals(null, udf.evaluate(null));

        assertEquals(WritableUtils.val(new String[] {}), udf.evaluate(WritableUtils.val(new String[] {})));

        assertEquals(WritableUtils.val(new String[] { "aaa:1.0" }), udf.evaluate(WritableUtils.val(new String[] { "aaa" })));

        assertEquals(WritableUtils.val(new String[] { "aaa:1.0" }), udf.evaluate(WritableUtils.val(new String[] { "aaa:1" })));

        assertEquals(WritableUtils.val(new String[] { "aaa:1.0" }), udf.evaluate(WritableUtils.val(new String[] { "aaa:1.0" })));

        float l2norm = MathUtils.l2norm(new float[] { 1.0f, 0.5f });
        assertEquals(WritableUtils.val(new String[] { "aaa:" + 1.0f / l2norm,
                "bbb:" + 0.5f / l2norm }), udf.evaluate(WritableUtils.val(new String[] { "aaa:1.0",
                "bbb:0.5" })));

        l2norm = MathUtils.l2norm(new float[] { 1.0f, -0.5f });
        assertEquals(WritableUtils.val(new String[] { "aaa:" + 1.0f / l2norm,
                "bbb:" + -0.5f / l2norm }), udf.evaluate(WritableUtils.val(new String[] {
                "aaa:1.0", "bbb:-0.5" })));

        List<Text> expected = udf.evaluate(WritableUtils.val(new String[] { "bbb:-0.5", "aaa:1.0" }));
        Collections.sort(expected);
        List<Text> actual = udf.evaluate(WritableUtils.val(new String[] { "aaa:1.0", "bbb:-0.5" }));
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

}
