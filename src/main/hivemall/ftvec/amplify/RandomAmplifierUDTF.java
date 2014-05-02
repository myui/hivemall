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
package hivemall.ftvec.amplify;

import hivemall.HivemallConstants;
import hivemall.common.RandomizedAmplifier;
import hivemall.common.RandomizedAmplifier.DropoutListener;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;

public class RandomAmplifierUDTF extends GenericUDTF implements DropoutListener<Object[]> {

    private transient ObjectInspector[] argOIs;

    private transient RandomizedAmplifier<Object[]> amplifier;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int numArgs = argOIs.length;
        if(numArgs < 3) {
            throw new UDFArgumentException("rand_amplify(int xtimes, int num_buffers, *) takes at least three arguments");
        }
        // xtimes
        if(argOIs[0].getTypeName() != HivemallConstants.INT_TYPE_NAME) {
            throw new UDFArgumentException("First argument must be int: " + argOIs[0].getTypeName());
        }
        if(!(argOIs[0] instanceof WritableConstantIntObjectInspector)) {
            throw new UDFArgumentException("WritableConstantIntObjectInspector is expected for the first argument: "
                    + argOIs[0].getClass().getSimpleName());
        }
        int xtimes = ((WritableConstantIntObjectInspector) argOIs[0]).getWritableConstantValue().get();
        if(!(xtimes >= 1)) {
            throw new UDFArgumentException("Illegal xtimes value: " + xtimes);
        }
        // num_buffers
        if(argOIs[1].getTypeName() != HivemallConstants.INT_TYPE_NAME) {
            throw new UDFArgumentException("Second argument must be int: "
                    + argOIs[1].getTypeName());
        }
        if(!(argOIs[1] instanceof WritableConstantIntObjectInspector)) {
            throw new UDFArgumentException("WritableConstantIntObjectInspector is expected for the second argument: "
                    + argOIs[1].getClass().getSimpleName());
        }
        int numBuffers = ((WritableConstantIntObjectInspector) argOIs[1]).getWritableConstantValue().get();
        if(numBuffers < 2) {
            throw new UDFArgumentException("num_buffers must be greater than 2: " + numBuffers);
        }
        this.argOIs = argOIs;

        this.amplifier = new RandomizedAmplifier<Object[]>(numBuffers, xtimes);
        amplifier.setDropoutListener(this);

        final ArrayList<String> fieldNames = new ArrayList<String>();
        final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        for(int i = 2; i < numArgs; i++) {
            fieldNames.add("c" + (i - 1));
            ObjectInspector rawOI = argOIs[i];
            ObjectInspector retOI = ObjectInspectorUtils.getStandardObjectInspector(rawOI, ObjectInspectorCopyOption.WRITABLE);
            fieldOIs.add(retOI);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final Object[] row = new Object[args.length - 2];
        for(int i = 2; i < args.length; i++) {
            Object arg = args[i];
            ObjectInspector argOI = argOIs[i];
            row[i - 2] = ObjectInspectorUtils.copyToStandardObject(arg, argOI);
        }
        amplifier.add(row);
    }

    @Override
    public void close() throws HiveException {
        amplifier.sweepAll();
        this.amplifier = null;
    }

    @Override
    public void onDrop(Object[] row) throws HiveException {
        forward(row);
    }

}
