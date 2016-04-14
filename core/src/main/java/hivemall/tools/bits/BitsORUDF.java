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
package hivemall.tools.bits;

import hivemall.utils.hadoop.HiveUtils;
import hivemall.utils.hadoop.WritableUtils;

import java.util.Arrays;
import java.util.BitSet;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(name = "bits_or",
        value = "_FUNC_(array<long> b1, array<long> b2, ..) - Returns a logical OR given bitsets")
public final class BitsORUDF extends GenericUDF {

    private ListObjectInspector[] _listOIs;
    private PrimitiveObjectInspector _listElemOI;

    private BitSet _bitset;

    public BitsORUDF() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        final int argLength = argOIs.length;
        if (argLength < 2) {
            throw new UDFArgumentLengthException("Expecting at least two arrays as arguments: "
                    + argLength);
        }

        ListObjectInspector[] argListOIs = new ListObjectInspector[argLength];
        ListObjectInspector arg0ListOI = HiveUtils.asListOI(argOIs[0]);
        PrimitiveObjectInspector arg0ElemOI = HiveUtils.asLongCompatibleOI(arg0ListOI.getListElementObjectInspector());

        argListOIs[0] = arg0ListOI;
        for (int i = 1; i < argLength; i++) {
            ListObjectInspector listOI = HiveUtils.asListOI(argOIs[i]);
            ObjectInspector elemOI = listOI.getListElementObjectInspector();
            if (!HiveUtils.isNumberOI(elemOI)) {
                throw new UDFArgumentException("Expecting number OI: " + elemOI.getTypeName());
            }
            argListOIs[i] = listOI;
        }

        this._listOIs = argListOIs;
        this._listElemOI = arg0ElemOI;
        this._bitset = new BitSet();

        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        for (int i = 0, numArgs = arguments.length; i < numArgs; i++) {
            final Object listObj = arguments[i].get();
            if (listObj == null) {
                continue;
            }
            final ListObjectInspector listOI = _listOIs[i];
            final int listlen = listOI.getListLength(listObj);
            long[] longs = new long[listlen];
            for (int j = 0; j < listlen; j++) {
                Object elem = listOI.getListElement(listObj, j);
                if (elem == null) {
                    throw new HiveException("Illegal Null value is found in bit representation");
                }
                long v = PrimitiveObjectInspectorUtils.getLong(elem, _listElemOI);
                longs[j] = v;
            }
            BitSet bs = BitSet.valueOf(longs);
            _bitset.or(bs);
        }

        long[] longs = _bitset.toLongArray();
        _bitset.clear();
        return WritableUtils.toWritableList(longs);
    }

    @Override
    public String getDisplayString(String[] children) {
        return "bits_or(" + Arrays.toString(children) + ")";
    }

}
