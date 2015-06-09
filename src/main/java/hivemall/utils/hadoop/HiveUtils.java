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
package hivemall.utils.hadoop;

import static hivemall.HivemallConstants.BIGINT_TYPE_NAME;
import static hivemall.HivemallConstants.BOOLEAN_TYPE_NAME;
import static hivemall.HivemallConstants.INT_TYPE_NAME;
import static hivemall.HivemallConstants.SMALLINT_TYPE_NAME;
import static hivemall.HivemallConstants.STRING_TYPE_NAME;
import static hivemall.HivemallConstants.TINYINT_TYPE_NAME;

import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public final class HiveUtils {

    private HiveUtils() {}

    public static int parseInt(@Nonnull final Object o) {
        if(o instanceof Integer) {
            return ((Integer) o).intValue();
        }
        if(o instanceof IntWritable) {
            return ((IntWritable) o).get();
        }
        if(o instanceof LongWritable) {
            long l = ((LongWritable) o).get();
            if(l > 0x7fffffffL) {
                throw new IllegalArgumentException("feature index must be less than "
                        + Integer.MAX_VALUE + ", but was " + l);
            }
            return (int) l;
        }
        String s = o.toString();
        return Integer.parseInt(s);
    }

    public static Text asText(@Nullable final Object o) {
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

    public static int asJavaInt(@Nullable final Object o, final int nullValue) {
        if(o == null) {
            return nullValue;
        }
        return asJavaInt(o);
    }

    public static int asJavaInt(@Nullable final Object o) {
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

    public static boolean isStringOI(@Nonnull final ObjectInspector oi) {
        String typeName = oi.getTypeName();
        return STRING_TYPE_NAME.equals(typeName);
    }

    public static boolean isIntOI(@Nonnull final ObjectInspector oi) {
        String typeName = oi.getTypeName();
        return INT_TYPE_NAME.equals(typeName);
    }

    public static boolean isBigIntOI(@Nonnull final ObjectInspector oi) {
        String typeName = oi.getTypeName();
        return BIGINT_TYPE_NAME.equals(typeName);
    }

    public static boolean isBooleanOI(@Nonnull final ObjectInspector oi) {
        String typeName = oi.getTypeName();
        return BOOLEAN_TYPE_NAME.equals(typeName);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <T extends Writable> T getConstValue(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        ConstantObjectInspector constOI = (ConstantObjectInspector) oi;
        Object v = constOI.getWritableConstantValue();
        return (T) v;
    }

    @Nonnull
    public static String[] getConstStringArray(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        ConstantObjectInspector constOI = (ConstantObjectInspector) oi;
        final List<?> lst = (List<?>) constOI.getWritableConstantValue();
        final int size = lst.size();
        final String[] ary = new String[size];
        for(int i = 0; i < size; i++) {
            Object o = lst.get(i);
            if(o != null) {
                ary[i] = o.toString();
            }
        }
        return ary;
    }

    public static String getConstString(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if(!isStringOI(oi)) {
            throw new UDFArgumentException("argument must be a Text value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        Text v = getConstValue(oi);
        return v == null ? null : v.toString();
    }

    public static boolean getConstBoolean(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if(!isBooleanOI(oi)) {
            throw new UDFArgumentException("argument must be a Boolean value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        BooleanWritable v = getConstValue(oi);
        return v.get();
    }

    public static int getConstInt(@Nonnull final ObjectInspector oi) throws UDFArgumentException {
        if(!isIntOI(oi)) {
            throw new UDFArgumentException("argument must be a Int value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        IntWritable v = getConstValue(oi);
        return v.get();
    }

    public static long getConstLong(@Nonnull final ObjectInspector oi) throws UDFArgumentException {
        if(!isBigIntOI(oi)) {
            throw new UDFArgumentException("argument must be a BigInt value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        LongWritable v = getConstValue(oi);
        return v.get();
    }

    public static long getAsConstLong(@Nonnull final ObjectInspector numberOI)
            throws UDFArgumentException {
        final String typeName = numberOI.getTypeName();
        if(BIGINT_TYPE_NAME.equals(typeName)) {
            LongWritable v = getConstValue(numberOI);
            return v.get();
        } else if(INT_TYPE_NAME.equals(typeName)) {
            IntWritable v = getConstValue(numberOI);
            return v.get();
        } else if(SMALLINT_TYPE_NAME.equals(typeName)) {
            ShortWritable v = getConstValue(numberOI);
            return v.get();
        } else if(TINYINT_TYPE_NAME.equals(typeName)) {
            ByteWritable v = getConstValue(numberOI);
            return v.get();
        }
        throw new UDFArgumentException("Unexpected argument type to cast as long: "
                + TypeInfoUtils.getTypeInfoFromObjectInspector(numberOI));
    }

    @Nonnull
    public static ConstantObjectInspector asConstantObjectInspector(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if(!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        return (ConstantObjectInspector) oi;
    }

    public static PrimitiveObjectInspector asPrimitiveObjectInspector(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if(oi.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("Is not PrimitiveObjectInspector: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        return (PrimitiveObjectInspector) oi;
    }

    public static IntObjectInspector asIntOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if(!INT_TYPE_NAME.equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be INT: " + argOI.getTypeName());
        }
        return (IntObjectInspector) argOI;
    }

    public static PrimitiveObjectInspector asIntCompatibleOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentTypeException {
        if(argOI.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
                    + argOI.getTypeName() + " is passed.");
        }
        final PrimitiveObjectInspector oi = (PrimitiveObjectInspector) argOI;
        switch (oi.getPrimitiveCategory()) {
            case INT:
            case SHORT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case BYTE:
            case STRING:
            case DECIMAL:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Unxpected type '" + argOI.getTypeName()
                        + "' is passed.");
        }
        return oi;
    }

    public static PrimitiveObjectInspector asDoubleCompatibleOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentTypeException {
        if(argOI.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
                    + argOI.getTypeName() + " is passed.");
        }
        final PrimitiveObjectInspector oi = (PrimitiveObjectInspector) argOI;
        switch (oi.getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case TIMESTAMP:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Only numeric or string type arguments are accepted but "
                        + argOI.getTypeName() + " is passed.");
        }
        return oi;
    }

    @Nonnull
    public static FloatWritable[] newFloatArray(final int size, final float defaultVal) {
        final FloatWritable[] array = new FloatWritable[size];
        for(int i = 0; i < size; i++) {
            array[i] = new FloatWritable(defaultVal);
        }
        return array;
    }

    public static LazySimpleSerDe getKeyValueLineSerde(@Nonnull final PrimitiveObjectInspector keyOI, @Nonnull final PrimitiveObjectInspector valueOI)
            throws SerDeException {
        LazySimpleSerDe serde = new LazySimpleSerDe();
        Configuration conf = new Configuration();
        Properties tbl = new Properties();
        tbl.setProperty("columns", "key,value");
        tbl.setProperty("columns.types", keyOI.getTypeName() + "," + valueOI.getTypeName());
        serde.initialize(conf, tbl);
        return serde;
    }

    public static LazySimpleSerDe getLineSerde(@Nonnull final PrimitiveObjectInspector... OIs)
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
