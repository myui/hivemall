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
package hivemall.mf;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

public class OnlineMatrixFactorizationUDTFTest {

    @Test
    public void test() throws HiveException {
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationSGDUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        //ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3 -eta 0.0002"));
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3"));
        ObjectInspector[] argOIs = new ObjectInspector[] { intOI, intOI, floatOI, param };
        mf.initialize(argOIs);

        float[][] rating = { { 5, 3, 0, 1 }, { 4, 0, 0, 1 }, { 1, 1, 0, 5 }, { 1, 0, 0, 4 },
                { 0, 1, 5, 4 } };
        Object[] args = new Object[3];
        final int num_iters = 1000;
        for(int iter = 0; iter < num_iters; iter++) {
            for(int row = 0; row < rating.length; row++) {
                for(int col = 0, size = rating[row].length; col < size; col++) {
                    //System.out.print(row + "," + col + ",");
                    args[0] = row;
                    args[1] = col;
                    args[2] = (float) rating[row][col];
                    //System.out.println((float) rating[row][col]);
                    mf.process(args);
                }
            }
        }
        for(int row = 0; row < rating.length; row++) {
            for(int col = 0, size = rating[row].length; col < size; col++) {
                double predicted = mf.predict(row, col);
                //System.out.print(rating[row][col] + "[" + predicted + "]\t");
                Assert.assertEquals(rating[row][col], predicted, 0.2d);
            }
            //System.out.println();
        }
    }

}
