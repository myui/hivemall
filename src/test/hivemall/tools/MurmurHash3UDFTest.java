/**
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
package hivemall.tools;

import hivemall.ftvec.hashing.MurmurHash3UDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.junit.Test;

public class MurmurHash3UDFTest {

    @Test
    public void testEvaluate() throws UDFArgumentException {
        MurmurHash3UDF udf = new MurmurHash3UDF();
        System.out.println(udf.evaluate("sadfdfjljlkajfla;few", 1 << 24));
        System.out.println(udf.evaluate("sadfdfjljlkajfla;fe", 1 << 24));
        System.out.println(udf.evaluate("adfdfjljlkajfla;few", 1 << 24));
        System.out.println(udf.evaluate("sadfdfjljlajfla;few", 1 << 24));
        System.out.println(udf.evaluate("1", 1 << 24));
        System.out.println(udf.evaluate("11", 1 << 24));
        System.out.println(udf.evaluate("2", 1 << 24));
        System.out.println(udf.evaluate("0", 1 << 24));
        System.out.println(udf.evaluate("sadfdfjljlajfla;fsasasaew", 1 << 24));
    }

}
