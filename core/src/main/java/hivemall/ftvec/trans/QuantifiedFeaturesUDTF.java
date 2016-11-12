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
package hivemall.ftvec.trans;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.lang.Identifier;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(
        name = "quantified_features",
        value = "_FUNC_(boolean output, col1, col2, ...) - Returns an identified features in a dence array<double>")
public final class QuantifiedFeaturesUDTF extends GenericUDTF {

    private BooleanObjectInspector boolOI;
    private PrimitiveObjectInspector[] doubleOIs;
    private Identifier<String>[] identifiers;
    private DoubleWritable[] columnValues;

    // lazy instantiation to avoid org.apache.hive.com.esotericsoftware.kryo.KryoException: java.lang.NullPointerException
    private transient Object[] fowardObjs;

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
        this.doubleOIs = new PrimitiveObjectInspector[outputSize];
        this.columnValues = new DoubleWritable[outputSize];
        this.identifiers = new Identifier[outputSize];
        this.fowardObjs = null;

        for (int i = 0; i < outputSize; i++) {
            columnValues[i] = new DoubleWritable(Double.NaN);
            ObjectInspector argOI = argOIs[i + 1];
            if (HiveUtils.isNumberOI(argOI)) {
                doubleOIs[i] = HiveUtils.asDoubleCompatibleOI(argOI);
            } else {
                identifiers[i] = new Identifier<String>();
            }
        }

        ArrayList<String> fieldNames = new ArrayList<String>(outputSize);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(outputSize);
        fieldNames.add("features");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (fowardObjs == null) {
            this.fowardObjs = new Object[] {Arrays.asList(columnValues)};
        }

        boolean outputRow = boolOI.get(args[0]);
        if (outputRow) {
            final DoubleWritable[] values = this.columnValues;
            for (int i = 0, outputSize = args.length - 1; i < outputSize; i++) {
                Object arg = args[i + 1];
                Identifier<String> identifier = identifiers[i];
                if (identifier == null) {
                    double v = PrimitiveObjectInspectorUtils.getDouble(arg, doubleOIs[i]);
                    values[i].set(v);
                } else {
                    if (arg == null) {
                        throw new HiveException("Found Null in the input: " + Arrays.toString(args));
                    } else {
                        String k = arg.toString();
                        int id = identifier.valueOf(k);
                        values[i].set(id);
                    }
                }
            }
            forward(fowardObjs);
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
        this.doubleOIs = null;
        this.identifiers = null;
        this.columnValues = null;
        this.fowardObjs = null;
    }

}
