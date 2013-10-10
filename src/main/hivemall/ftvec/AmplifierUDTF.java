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
package hivemall.ftvec;

import hivemall.common.HivemallConstants;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;

public class AmplifierUDTF extends GenericUDTF {

    private int xtimes;
    private Object[] forwardObjs;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(!(argOIs.length >= 2)) {
            throw new UDFArgumentException("amplifier(int xtimes, *) takes at least two arguments");
        }
        if(argOIs[0].getTypeName() != HivemallConstants.INT_TYPE_NAME) {
            throw new UDFArgumentException("first argument must be int: " + argOIs[0].getTypeName());
        }
        if(!(argOIs[0] instanceof WritableConstantIntObjectInspector)) {
            throw new UDFArgumentException("WritableConstantIntObjectInspector is expected for the first argument: "
                    + argOIs[0].getClass().getSimpleName());
        }
        this.xtimes = ((WritableConstantIntObjectInspector) argOIs[0]).getWritableConstantValue().get();
        if(!(xtimes >= 1)) {
            throw new UDFArgumentException("Illegal xtimes value: " + xtimes);
        }
        this.forwardObjs = new Object[argOIs.length - 1];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        for(int i = 1; i < argOIs.length; i++) {
            fieldNames.add("c" + i);
            fieldOIs.add(argOIs[i]);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final Object[] forwardObjs = this.forwardObjs;
        for(int i = 1; i < args.length; i++) {
            forwardObjs[i - 1] = args[i];
        }
        for(int x = 0; x < xtimes; x++) {
            forward(forwardObjs);
        }
    }

    @Override
    public void close() throws HiveException {
        this.forwardObjs = null;
    }

}
