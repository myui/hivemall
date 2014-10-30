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

@Description(name = "generate_series", value = "_FUNC_(const int|bigint start, const int|bigint end) - Generate a series of values, from start to end")
public final class GenerateSeriesUDTF extends GenericUDTF {

    private long start, end;
    private boolean useBigInt;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2) {
            throw new UDFArgumentException("Expected number of arguments is 2: " + argOIs.length);
        }

        ArrayList<String> fieldNames = new ArrayList<String>(1);
        fieldNames.add("value");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);

        this.useBigInt = HiveUtils.isBigInt(argOIs[1]);
        if(useBigInt) {
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        } else {
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        }

        this.start = HiveUtils.getAsConstLong(argOIs[0]);
        this.end = HiveUtils.getAsConstLong(argOIs[1]);
        if(start > end) {
            throw new UDFArgumentException("start '" + start
                    + "' must be less than or equlas to end '" + end + "'");
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] argOIs) throws HiveException {
        final Object[] forwardObjs = new Object[1];
        if(useBigInt) {
            if(start == end) {
                forwardObjs[0] = start;
                forward(forwardObjs);
            } else {
                for(long i = start; i <= end; i++) {
                    forwardObjs[0] = i;
                    forward(forwardObjs);
                }
            }
        } else {
            int starti = (int) start;
            int endi = (int) end;
            if(starti == endi) {
                forwardObjs[0] = starti;
                forward(forwardObjs);
            } else {
                for(int i = starti; i <= endi; i++) {
                    forwardObjs[0] = i;
                    forward(forwardObjs);
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {}

}
