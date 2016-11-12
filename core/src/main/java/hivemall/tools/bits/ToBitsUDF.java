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
package hivemall.tools.bits;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

@Description(
        name = "to_bits",
        value = "_FUNC_(int[] indexes) - Returns an bitset representation if the given indexes in long[]")
@UDFType(deterministic = true, stateful = false)
public final class ToBitsUDF extends GenericUDF {

    private ListObjectInspector listOI;
    private PrimitiveObjectInspector listElemOI;

    private BitSet bitset;

    public ToBitsUDF() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 1) {
            throw new UDFArgumentLengthException(
                "Expected 1 argument for _FUNC_(int[] indexes) but got " + argOIs.length
                        + " arguments");
        }
        this.listOI = HiveUtils.asListOI(argOIs[0]);
        this.listElemOI = HiveUtils.asIntCompatibleOI(listOI.getListElementObjectInspector());
        this.bitset = new BitSet();

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    }

    @Override
    public List<LongWritable> evaluate(DeferredObject[] args) throws HiveException {
        assert (args.length == 1);
        Object arg0 = args[0].get();
        if (arg0 == null) {
            return null;
        }

        HiveUtils.setBits(arg0, listOI, listElemOI, bitset);
        long[] array = bitset.toLongArray();
        bitset.clear();

        return WritableUtils.toWritableList(array);
    }


    @Override
    public String getDisplayString(String[] args) {
        return "to_bits(" + Arrays.toString(args) + ")";
    }
}
