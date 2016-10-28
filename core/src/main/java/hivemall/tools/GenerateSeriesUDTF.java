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
package hivemall.tools;

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "generate_series",
        value = "_FUNC_(const int|bigint start, const int|bigint end) - "
                + "Generate a series of values, from start to end")
public final class GenerateSeriesUDTF extends GenericUDTF {

    private long start, end;
    private boolean useBigInt;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2) {
            throw new UDFArgumentException("Expected number of arguments is 2: " + argOIs.length);
        }

        ArrayList<String> fieldNames = new ArrayList<String>(1);
        fieldNames.add("value");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);

        this.useBigInt = HiveUtils.isBigIntOI(argOIs[1]);
        if (useBigInt) {
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        } else {
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        }

        this.start = HiveUtils.getAsConstLong(argOIs[0]);
        this.end = HiveUtils.getAsConstLong(argOIs[1]);
        if (start > end) {
            throw new UDFArgumentException("start '" + start
                    + "' must be less than or equlas to end '" + end + "'");
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] argOIs) throws HiveException {
        final Object[] forwardObjs = new Object[1];
        if (useBigInt) {
            if (start == end) {
                forwardObjs[0] = start;
                forward(forwardObjs);
            } else {
                for (long i = start; i <= end; i++) {
                    forwardObjs[0] = i;
                    forward(forwardObjs);
                }
            }
        } else {
            int starti = (int) start;
            int endi = (int) end;
            if (starti == endi) {
                forwardObjs[0] = starti;
                forward(forwardObjs);
            } else {
                for (int i = starti; i <= endi; i++) {
                    forwardObjs[0] = i;
                    forward(forwardObjs);
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {}

}
