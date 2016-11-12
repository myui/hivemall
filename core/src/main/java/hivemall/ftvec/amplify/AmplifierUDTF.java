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
package hivemall.ftvec.amplify;

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "amplify",
        value = "_FUNC_(const int xtimes, *) - amplify the input records x-times")
public final class AmplifierUDTF extends GenericUDTF {

    private int xtimes;
    private Object[] forwardObjs;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (!(argOIs.length >= 2)) {
            throw new UDFArgumentException("_FUNC_(int xtimes, *) takes at least two arguments");
        }
        this.xtimes = HiveUtils.getAsConstInt(argOIs[0]);
        if (!(xtimes >= 1)) {
            throw new UDFArgumentException("Illegal xtimes value: " + xtimes);
        }
        this.forwardObjs = new Object[argOIs.length - 1];

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        for (int i = 1; i < argOIs.length; i++) {
            fieldNames.add("c" + i);
            fieldOIs.add(argOIs[i]);
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        final Object[] forwardObjs = this.forwardObjs;
        for (int i = 1; i < args.length; i++) {
            forwardObjs[i - 1] = args[i];
        }
        for (int x = 0; x < xtimes; x++) {
            forward(forwardObjs);
        }
    }

    @Override
    public void close() throws HiveException {
        this.forwardObjs = null;
    }

}
