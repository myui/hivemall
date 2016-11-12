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
package hivemall.ftvec.conv;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Identifier;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(name = "quantify",
        value = "_FUNC_(boolean outout, col1, col2, ...) - Returns an identified features")
public final class QuantifyColumnsUDTF extends GenericUDTF {

    private BooleanObjectInspector boolOI;
    private Identifier<String>[] identifiers;
    private Object[] forwardObjs;
    private IntWritable[] forwardIntObjs;

    @SuppressWarnings("unchecked")
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        int size = argOIs.length;
        if (size < 2) {
            throw new UDFArgumentException("quantified_features takes at least two arguments: "
                    + size);
        }
        this.boolOI = HiveUtils.asBooleanOI(argOIs[0]);

        int outputSize = size - 1;
        this.forwardObjs = new Object[outputSize];
        this.forwardIntObjs = new IntWritable[outputSize];
        this.identifiers = new Identifier[outputSize];

        final ArrayList<String> fieldNames = new ArrayList<String>(outputSize);
        final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(outputSize);

        for (int i = 0; i < outputSize; i++) {
            fieldNames.add("c" + i);
            ObjectInspector argOI = argOIs[i + 1];
            if (HiveUtils.isNumberOI(argOI)) {
                fieldOIs.add(argOI);
            } else {
                identifiers[i] = new Identifier<String>();
                forwardIntObjs[i] = new IntWritable(-1);
                fieldOIs.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
            }
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        boolean outputRow = boolOI.get(args[0]);
        if (outputRow) {
            final Object[] forwardObjs = this.forwardObjs;
            for (int i = 0, outputSize = args.length - 1; i < outputSize; i++) {
                Object arg = args[i + 1];
                Identifier<String> identifier = identifiers[i];
                if (identifier == null) {
                    forwardObjs[i] = arg;
                } else {
                    if (arg == null) {
                        forwardObjs[i] = null;
                    } else {
                        String k = arg.toString();
                        int id = identifier.valueOf(k);
                        IntWritable o = forwardIntObjs[i];
                        o.set(id);
                        forwardObjs[i] = o;
                    }
                }
            }
            forward(forwardObjs);
        } else {// load only  
            for (int i = 0, outputSize = args.length - 1; i < outputSize; i++) {
                Identifier<String> identifier = identifiers[i];
                if (identifier != null) {
                    Object arg = args[i + 1];
                    if (arg != null) {
                        String k = arg.toString();
                        identifier.valueOf(k);
                    }
                }
            }
        }

    }

    @Override
    public void close() throws HiveException {
        this.boolOI = null;
        this.identifiers = null;
        this.forwardObjs = null;
        this.forwardIntObjs = null;
    }

}
