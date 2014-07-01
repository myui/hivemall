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
package hivemall.utils.hadoop;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public final class HiveUtils {

    public static Text asText(final Object o) {
        if(o == null) {
            return null;
        }
        if(o instanceof Text) {
            return (Text) o;
        }
        if(o instanceof LazyString) {
            LazyString l = (LazyString) o;
            return l.getWritableObject();
        }
        if(o instanceof String) {
            String s = (String) o;
            return new Text(s);
        }
        String s = o.toString();
        return new Text(s);
    }

    public static int asJavaInt(final Object o, final int nullValue) {
        if(o == null) {
            return nullValue;
        }
        return asJavaInt(o);
    }

    public static int asJavaInt(final Object o) {
        if(o == null) {
            throw new IllegalArgumentException();
        }
        if(o instanceof Integer) {
            return ((Integer) o).intValue();
        }
        if(o instanceof LazyInteger) {
            IntWritable i = ((LazyInteger) o).getWritableObject();
            return i.get();
        }
        if(o instanceof IntWritable) {
            return ((IntWritable) o).get();
        }
        String s = o.toString();
        return Integer.parseInt(s);
    }

    public static String getConstString(ObjectInspector oi) throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        WritableConstantStringObjectInspector stringOI = (WritableConstantStringObjectInspector) oi;
        return stringOI.getWritableConstantValue().toString();
    }

    public static Object getConstValue(ObjectInspector oi) throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        return ((ConstantObjectInspector) oi).getWritableConstantValue();
    }

    public static PrimitiveObjectInspector asPrimitiveObjectInspector(ObjectInspector oi)
            throws UDFArgumentException {
        if(oi.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("Is not PrimitiveObjectInspector: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        return (PrimitiveObjectInspector) oi;
    }
}
