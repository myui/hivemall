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
package hivemall.tools.array;

import hivemall.utils.hadoop.HiveUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

@Description(name = "array_intersect",
        value = "_FUNC_(x1, x2, ..) - Returns an intersect of given arrays")
@UDFType(deterministic = true, stateful = false)
public final class ArrayIntersectUDF extends GenericUDF {

    private ListObjectInspector[] argListOIs;
    private List<Object> result;

    public ArrayIntersectUDF() {}

    @Override
    public ObjectInspector initialize(@Nonnull ObjectInspector[] argOIs)
            throws UDFArgumentException {
        final int argLength = argOIs.length;
        if (argLength < 2) {
            throw new UDFArgumentLengthException("Expecting at least two arrays as arguments: "
                    + argLength);
        }

        ListObjectInspector[] argListOIs = new ListObjectInspector[argLength];
        ListObjectInspector arg0ListOI = HiveUtils.asListOI(argOIs[0]);;
        ObjectInspector arg0ElemOI = arg0ListOI.getListElementObjectInspector();
        argListOIs[0] = arg0ListOI;
        for (int i = 1; i < argLength; i++) {
            ListObjectInspector listOI = HiveUtils.asListOI(argOIs[i]);
            if (!ObjectInspectorUtils.compareTypes(listOI.getListElementObjectInspector(),
                arg0ElemOI)) {
                throw new UDFArgumentException("Array types does not match: "
                        + arg0ElemOI.getTypeName() + " != "
                        + listOI.getListElementObjectInspector().getTypeName());
            }
            argListOIs[i] = listOI;
        }

        this.argListOIs = argListOIs;
        this.result = new ArrayList<Object>();
        return ObjectInspectorUtils.getStandardObjectInspector(arg0ListOI);
    }

    @Override
    public List<Object> evaluate(@Nonnull DeferredObject[] args) throws HiveException {
        result.clear();

        final Object arg0 = args[0].get();
        if (arg0 == null) {
            return Collections.emptyList();
        }

        Set<InspectableObject> checkSet = new HashSet<ArrayIntersectUDF.InspectableObject>();
        final ListObjectInspector arg0ListOI = argListOIs[0];
        final ObjectInspector arg0ElemOI = arg0ListOI.getListElementObjectInspector();
        final int arg0size = arg0ListOI.getListLength(arg0);
        for (int i = 0; i < arg0size; i++) {
            Object o = arg0ListOI.getListElement(arg0, i);
            if (o == null) {
                continue;
            }
            checkSet.add(new InspectableObject(o, arg0ElemOI));
        }

        final InspectableObject probe = new InspectableObject();
        for (int i = 1, numArgs = args.length; i < numArgs; i++) {
            final Object argI = args[i].get();
            if (argI == null) {
                continue;
            }
            final Set<InspectableObject> newSet = new HashSet<ArrayIntersectUDF.InspectableObject>();
            final ListObjectInspector argIListOI = argListOIs[i];
            final ObjectInspector argIElemOI = argIListOI.getListElementObjectInspector();
            for (int j = 0, j_size = argIListOI.getListLength(argI); j < j_size; j++) {
                Object o = argIListOI.getListElement(argI, j);
                if (o == null) {
                    continue;
                }
                probe.set(o, argIElemOI);
                if (checkSet.contains(probe)) {
                    newSet.add(probe.copy());
                }
            }
            checkSet = newSet;
        }

        for (InspectableObject inspect : checkSet) {
            Object obj = ObjectInspectorUtils.copyToStandardObject(inspect.o, inspect.oi);
            result.add(obj);
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "array_intersect(" + Arrays.toString(args) + ")";
    }

    private static final class InspectableObject implements Comparable<InspectableObject> {
        public Object o;
        public ObjectInspector oi;

        InspectableObject() {}

        InspectableObject(@Nonnull Object o, @Nonnull ObjectInspector oi) {
            this.o = o;
            this.oi = oi;
        }

        void set(@Nonnull Object o, @Nonnull ObjectInspector oi) {
            this.o = o;
            this.oi = oi;
        }

        InspectableObject copy() {
            return new InspectableObject(o, oi);
        }

        @Override
        public int hashCode() {
            return ObjectInspectorUtils.hashCode(o, oi);
        }

        @Override
        public int compareTo(InspectableObject otherOI) {
            return ObjectInspectorUtils.compare(o, oi, otherOI.o, otherOI.oi);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof InspectableObject)) {
                return false;
            }
            return compareTo((InspectableObject) other) == 0;
        }

    }
}
