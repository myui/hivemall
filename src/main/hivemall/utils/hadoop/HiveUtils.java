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

import static hivemall.HivemallConstants.BIGINT_TYPE_NAME;
import static hivemall.HivemallConstants.BOOLEAN_TYPE_NAME;
import static hivemall.HivemallConstants.INT_TYPE_NAME;
import static hivemall.HivemallConstants.SMALLINT_TYPE_NAME;
import static hivemall.HivemallConstants.STRING_TYPE_NAME;
import static hivemall.HivemallConstants.TINYINT_TYPE_NAME;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector;
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

    public static boolean getConstBoolean(ObjectInspector oi) throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        String typeName = oi.getTypeName();
        if(!BOOLEAN_TYPE_NAME.equals(typeName)) {
            throw new UDFArgumentException("argument must be a boolean value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        WritableConstantBooleanObjectInspector booleanOI = (WritableConstantBooleanObjectInspector) oi;
        return booleanOI.getWritableConstantValue().get();
    }

    public static boolean isBigInt(ObjectInspector oi) {
        String typeName = oi.getTypeName();
        return BIGINT_TYPE_NAME.equals(typeName);
    }

    public static int getConstInt(ObjectInspector oi) throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        String typeName = oi.getTypeName();
        if(!INT_TYPE_NAME.equals(typeName)) {
            throw new UDFArgumentException("argument must be a int value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        WritableConstantIntObjectInspector intOI = (WritableConstantIntObjectInspector) oi;
        return intOI.getWritableConstantValue().get();
    }

    public static long getConstLong(ObjectInspector oi) throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        String typeName = oi.getTypeName();
        if(!BIGINT_TYPE_NAME.equals(typeName)) {
            throw new UDFArgumentException("argument must be a bigint value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        WritableConstantLongObjectInspector longOI = (WritableConstantLongObjectInspector) oi;
        return longOI.getWritableConstantValue().get();
    }

    public static long getAsConstLong(ObjectInspector numberOI) throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(numberOI)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(numberOI));
        }
        String typeName = numberOI.getTypeName();
        if(BIGINT_TYPE_NAME.equals(typeName)) {
            WritableConstantLongObjectInspector longOI = (WritableConstantLongObjectInspector) numberOI;
            return longOI.getWritableConstantValue().get();
        } else if(INT_TYPE_NAME.equals(typeName)) {
            WritableConstantIntObjectInspector intOI = (WritableConstantIntObjectInspector) numberOI;
            return (long) intOI.getWritableConstantValue().get();
        } else if(SMALLINT_TYPE_NAME.equals(typeName)) {
            WritableConstantShortObjectInspector shortOI = (WritableConstantShortObjectInspector) numberOI;
            return (long) shortOI.getWritableConstantValue().get();
        } else if(TINYINT_TYPE_NAME.equals(typeName)) {
            WritableConstantByteObjectInspector byteOI = (WritableConstantByteObjectInspector) numberOI;
            return (long) byteOI.getWritableConstantValue().get();
        }
        throw new UDFArgumentException("Unexpected argument type to cast as long: "
                + TypeInfoUtils.getTypeInfoFromObjectInspector(numberOI));
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

    public static boolean isStringOI(ObjectInspector oi) {
        String typeName = oi.getTypeName();
        return STRING_TYPE_NAME.equals(typeName);
    }

    public static LazySimpleSerDe getKeyValueLineSerde(PrimitiveObjectInspector keyOI, PrimitiveObjectInspector valueOI)
            throws SerDeException {
        LazySimpleSerDe serde = new LazySimpleSerDe();
        Configuration conf = new Configuration();
        Properties tbl = new Properties();
        tbl.setProperty("columns", "key,value");
        tbl.setProperty("columns.types", keyOI.getTypeName() + "," + valueOI.getTypeName());
        serde.initialize(conf, tbl);
        return serde;
    }

    public static LazySimpleSerDe getLineSerde(PrimitiveObjectInspector... OIs)
            throws SerDeException {
        if(OIs.length == 0) {
            throw new IllegalArgumentException("OIs must be specified");
        }
        LazySimpleSerDe serde = new LazySimpleSerDe();
        Configuration conf = new Configuration();
        Properties tbl = new Properties();

        StringBuilder columnNames = new StringBuilder();
        StringBuilder columnTypes = new StringBuilder();
        for(int i = 0; i < OIs.length; i++) {
            columnNames.append('c').append(i + 1).append(',');
            columnTypes.append(OIs[i].getTypeName()).append(',');
        }
        columnNames.deleteCharAt(columnNames.length() - 1);
        columnTypes.deleteCharAt(columnTypes.length() - 1);

        tbl.setProperty("columns", columnNames.toString());
        tbl.setProperty("columns.types", columnTypes.toString());
        serde.initialize(conf, tbl);
        return serde;
    }
}
