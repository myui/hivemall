/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(name = "quantified_features", value = "_FUNC_(col1, col2, ...) - Returns an identified features in a dence array<double>")
public final class QuantifiedFeaturesUDTF extends GenericUDTF {

    private PrimitiveObjectInspector[] doubleOIs;
    private Identifier<String>[] identifiers;
    private DoubleWritable[] columnValues;

    // org.apache.hive.com.esotericsoftware.kryo.KryoException: java.lang.NullPointerException
    private transient Object[] fowardObjs;

    @SuppressWarnings("unchecked")
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        int size = argOIs.length;
        this.doubleOIs = new PrimitiveObjectInspector[size];
        this.columnValues = new DoubleWritable[size];
        this.identifiers = new Identifier[size];
        this.fowardObjs = null;
        
        for(int i = 0; i < size; i++) {
            columnValues[i] = new DoubleWritable(Double.NaN);
            if(HiveUtils.isNumberOI(argOIs[i])) {
                doubleOIs[i] = HiveUtils.asDoubleCompatibleOI(argOIs[i]);
            } else {
                identifiers[i] = new Identifier<String>();
            }
        }


        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("features");
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if(fowardObjs == null) {
            this.fowardObjs = new Object[] { Arrays.asList(columnValues) };
        }
        
        final DoubleWritable[] values = this.columnValues;
        for(int i = 0; i < args.length; i++) {
            Object arg = args[i];

            Identifier<String> identifier = identifiers[i];
            if(identifier == null) {
                double v = PrimitiveObjectInspectorUtils.getDouble(arg, doubleOIs[i]);
                values[i].set(v);
            } else {
                if(arg == null) {
                    throw new HiveException("Found Null in the input: " + Arrays.toString(args));
                } else {
                    String k = arg.toString();
                    int id = identifier.valueOf(k);
                    values[i].set(id);
                }
            }

        }
        forward(fowardObjs);
    }

    @Override
    public void close() throws HiveException {
        this.doubleOIs = null;
        this.identifiers = null;
        this.columnValues = null;
    }

}
