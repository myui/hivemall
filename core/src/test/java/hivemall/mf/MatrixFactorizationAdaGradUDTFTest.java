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
package hivemall.mf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class MatrixFactorizationAdaGradUDTFTest {
    private static final boolean DEBUG_PRINT = false;

    private static void print(String msg) {
        if (DEBUG_PRINT)
            System.out.print(msg);
    }

    private static void println(String msg) {
        if (DEBUG_PRINT)
            System.out.println(msg);
    }

    private static void println() {
        if (DEBUG_PRINT)
            System.out.println();
    }

    @Test
    public void test() throws HiveException {
        println("--------------------------\n test()");
        OnlineMatrixFactorizationUDTF mf = new MatrixFactorizationAdaGradUDTF();

        ObjectInspector intOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        ObjectInspector floatOI = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
        ObjectInspector param = ObjectInspectorUtils.getConstantObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, new String("-factor 3"));
        ObjectInspector[] argOIs = new ObjectInspector[] {intOI, intOI, floatOI, param};
        mf.initialize(argOIs);

        float[][] rating = { {5, 3, 0, 1}, {4, 0, 0, 1}, {1, 1, 0, 5}, {1, 0, 0, 4}, {0, 1, 5, 4}};
        Object[] args = new Object[3];
        final int num_iters = 100;
        for (int iter = 0; iter < num_iters; iter++) {
            for (int row = 0; row < rating.length; row++) {
                for (int col = 0, size = rating[row].length; col < size; col++) {
                    //print(row + "," + col + ",");
                    args[0] = row;
                    args[1] = col;
                    args[2] = (float) rating[row][col];
                    //println((float) rating[row][col]);
                    mf.process(args);
                }
            }
        }
        for (int row = 0; row < rating.length; row++) {
            for (int col = 0, size = rating[row].length; col < size; col++) {
                double predicted = mf.predict(row, col);
                print(rating[row][col] + "[" + predicted + "]\t");
                Assert.assertEquals(rating[row][col], predicted, 0.2d);
            }
            println();
        }
    }

}
