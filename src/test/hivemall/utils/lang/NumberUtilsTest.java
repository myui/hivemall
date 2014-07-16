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
package hivemall.utils.lang;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class NumberUtilsTest {

    @Test
    public void testParseInt() {
        String s1 = "100";
        String s2 = "1k";
        String s3 = "2k";
        String s4 = "1m";
        String s5 = "1g";
        String s6 = "2g";
        String s7 = "2K";
        assertEquals(100, NumberUtils.parseInt(s1));
        assertEquals(1000, NumberUtils.parseInt(s2));
        assertEquals(2000, NumberUtils.parseInt(s3));
        assertEquals(1000000, NumberUtils.parseInt(s4));
        assertEquals(1000000000, NumberUtils.parseInt(s5));
        assertEquals(2000000000, NumberUtils.parseInt(s6));
        assertEquals(2000, NumberUtils.parseInt(s7));
    }

}
