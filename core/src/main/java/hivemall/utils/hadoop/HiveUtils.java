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
import static hivemall.HivemallConstants.BINARY_TYPE_NAME;
import static hivemall.HivemallConstants.BOOLEAN_TYPE_NAME;
import static hivemall.HivemallConstants.DOUBLE_TYPE_NAME;
import static hivemall.HivemallConstants.FLOAT_TYPE_NAME;
import static hivemall.HivemallConstants.INT_TYPE_NAME;
import static hivemall.HivemallConstants.SMALLINT_TYPE_NAME;
import static hivemall.HivemallConstants.STRING_TYPE_NAME;
import static hivemall.HivemallConstants.TINYINT_TYPE_NAME;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
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
        if (o instanceof Integer) {
            return ((Integer) o).intValue();
        }
        if (o instanceof IntWritable) {
            return ((IntWritable) o).get();
        }
        if (o instanceof LongWritable) {
            long l = ((LongWritable) o).get();
            if (l > 0x7fffffffL) {
                throw new IllegalArgumentException("feature index must be less than "
                        + Integer.MAX_VALUE + ", but was " + l);
            }
            return (int) l;
        }
        String s = o.toString();
        return Integer.parseInt(s);
    }

    public static Text asText(@Nullable final Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Text) {
            return (Text) o;
        }
        if (o instanceof LazyString) {
            LazyString l = (LazyString) o;
            return l.getWritableObject();
        }
        if (o instanceof String) {
            String s = (String) o;
            return new Text(s);
        }
        String s = o.toString();
        return new Text(s);
    }

    public static int asJavaInt(@Nullable final Object o, final int nullValue) {
        if (o == null) {
            return nullValue;
        }
        return asJavaInt(o);
    }

    public static int asJavaInt(@Nullable final Object o) {
        if (o == null) {
            throw new IllegalArgumentException();
        }
        if (o instanceof Integer) {
            return ((Integer) o).intValue();
        }
        if (o instanceof LazyInteger) {
            IntWritable i = ((LazyInteger) o).getWritableObject();
            return i.get();
        }
        if (o instanceof IntWritable) {
            return ((IntWritable) o).get();
        }
        String s = o.toString();
        return Integer.parseInt(s);
    }

    @Nullable
    public static List<String> asStringList(@Nonnull final DeferredObject arg,
            @Nonnull final ListObjectInspector listOI) throws HiveException {
        Object argObj = arg.get();
        if (argObj == null) {
            return null;
        }
        List<?> data = listOI.getList(argObj);
        int size = data.size();
        if (size == 0) {
            return Collections.emptyList();
        }
        final String[] ary = new String[size];
        for (int i = 0; i < size; i++) {
            Object o = data.get(i);
            if (o != null) {
                ary[i] = o.toString();
            }
        }
        return Arrays.asList(ary);
    }

    public static boolean isPrimitiveOI(@Nonnull final ObjectInspector oi) {
        return oi.getCategory() == Category.PRIMITIVE;
    }

    public static boolean isStructOI(@Nonnull final ObjectInspector oi) {
        return oi.getCategory() == Category.STRUCT;
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

    public static boolean isNumberOI(@Nonnull final ObjectInspector argOI) {
        if (argOI.getCategory() != Category.PRIMITIVE) {
            return false;
        }
        final PrimitiveObjectInspector oi = (PrimitiveObjectInspector) argOI;
        switch (oi.getPrimitiveCategory()) {
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BYTE:
                //case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    public static boolean isIntegerOI(@Nonnull final ObjectInspector argOI) {
        if (argOI.getCategory() != Category.PRIMITIVE) {
            return false;
        }
        final PrimitiveObjectInspector oi = (PrimitiveObjectInspector) argOI;
        switch (oi.getPrimitiveCategory()) {
            case INT:
            case SHORT:
            case LONG:
            case BYTE:
                return true;
            default:
                return false;
        }
    }

    @Nonnull
    public static boolean isListOI(@Nonnull final ObjectInspector oi) {
        Category category = oi.getCategory();
        return category == Category.LIST;
    }

    public static boolean isNumberListOI(@Nonnull final ObjectInspector oi){
        return isListOI(oi) && isNumberOI(((ListObjectInspector)oi).getListElementObjectInspector());
    }

    public static boolean isPrimitiveTypeInfo(@Nonnull TypeInfo typeInfo) {
        return typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE;
    }

    public static boolean isStructTypeInfo(@Nonnull TypeInfo typeInfo) {
        return typeInfo.getCategory() == ObjectInspector.Category.STRUCT;
    }

    public static boolean isNumberTypeInfo(@Nonnull TypeInfo typeInfo) {
        if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            return false;
        }
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public static boolean isBooleanTypeInfo(@Nonnull TypeInfo typeInfo) {
        if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            return false;
        }
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN:
                return true;
            default:
                return false;
        }
    }

    public static boolean isIntegerTypeInfo(@Nonnull TypeInfo typeInfo) {
        if (typeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            return false;
        }
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return true;
            default:
                return false;
        }
    }

    public static boolean isConstString(@Nonnull final ObjectInspector oi) {
        return ObjectInspectorUtils.isConstantObjectInspector(oi) && isStringOI(oi);
    }

    @Nonnull
    public static ListTypeInfo asListTypeInfo(@Nonnull TypeInfo typeInfo)
            throws UDFArgumentException {
        if (!typeInfo.getCategory().equals(Category.LIST)) {
            throw new UDFArgumentException("Expected list type: " + typeInfo);
        }
        return (ListTypeInfo) typeInfo;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <T extends Writable> T getConstValue(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if (!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        ConstantObjectInspector constOI = (ConstantObjectInspector) oi;
        Object v = constOI.getWritableConstantValue();
        return (T) v;
    }

    @Nullable
    public static String[] getConstStringArray(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if (!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        ConstantObjectInspector constOI = (ConstantObjectInspector) oi;
        if (constOI.getCategory() != Category.LIST) {
            throw new UDFArgumentException("argument must be an array: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        final List<?> lst = (List<?>) constOI.getWritableConstantValue();
        if (lst == null) {
            return null;
        }
        final int size = lst.size();
        final String[] ary = new String[size];
        for (int i = 0; i < size; i++) {
            Object o = lst.get(i);
            if (o != null) {
                ary[i] = o.toString();
            }
        }
        return ary;
    }

    public static String getConstString(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if (!isStringOI(oi)) {
            throw new UDFArgumentException("argument must be a Text value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        Text v = getConstValue(oi);
        return v == null ? null : v.toString();
    }

    public static boolean getConstBoolean(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        if (!isBooleanOI(oi)) {
            throw new UDFArgumentException("argument must be a Boolean value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        BooleanWritable v = getConstValue(oi);
        return v.get();
    }

    public static int getConstInt(@Nonnull final ObjectInspector oi) throws UDFArgumentException {
        if (!isIntOI(oi)) {
            throw new UDFArgumentException("argument must be a Int value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        IntWritable v = getConstValue(oi);
        return v.get();
    }

    public static long getConstLong(@Nonnull final ObjectInspector oi) throws UDFArgumentException {
        if (!isBigIntOI(oi)) {
            throw new UDFArgumentException("argument must be a BigInt value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        LongWritable v = getConstValue(oi);
        return v.get();
    }

    public static int getAsConstInt(@Nonnull final ObjectInspector numberOI)
            throws UDFArgumentException {
        final String typeName = numberOI.getTypeName();
        if (INT_TYPE_NAME.equals(typeName)) {
            IntWritable v = getConstValue(numberOI);
            return v.get();
        } else if (BIGINT_TYPE_NAME.equals(typeName)) {
            LongWritable v = getConstValue(numberOI);
            return (int) v.get();
        } else if (SMALLINT_TYPE_NAME.equals(typeName)) {
            ShortWritable v = getConstValue(numberOI);
            return v.get();
        } else if (TINYINT_TYPE_NAME.equals(typeName)) {
            ByteWritable v = getConstValue(numberOI);
            return v.get();
        }
        throw new UDFArgumentException("Unexpected argument type to cast as INT: "
                + TypeInfoUtils.getTypeInfoFromObjectInspector(numberOI));
    }

    public static long getAsConstLong(@Nonnull final ObjectInspector numberOI)
            throws UDFArgumentException {
        final String typeName = numberOI.getTypeName();
        if (BIGINT_TYPE_NAME.equals(typeName)) {
            LongWritable v = getConstValue(numberOI);
            return v.get();
        } else if (INT_TYPE_NAME.equals(typeName)) {
            IntWritable v = getConstValue(numberOI);
            return v.get();
        } else if (SMALLINT_TYPE_NAME.equals(typeName)) {
            ShortWritable v = getConstValue(numberOI);
            return v.get();
        } else if (TINYINT_TYPE_NAME.equals(typeName)) {
            ByteWritable v = getConstValue(numberOI);
            return v.get();
        }
        throw new UDFArgumentException("Unexpected argument type to cast as long: "
                + TypeInfoUtils.getTypeInfoFromObjectInspector(numberOI));
    }

    public static float getAsConstFloat(@Nonnull final ObjectInspector numberOI)
            throws UDFArgumentException {
        final String typeName = numberOI.getTypeName();
        if (FLOAT_TYPE_NAME.equals(typeName)) {
            FloatWritable v = getConstValue(numberOI);
            return v.get();
        } else if (DOUBLE_TYPE_NAME.equals(typeName)) {
            DoubleWritable v = getConstValue(numberOI);
            return (float) v.get();
        } else if (INT_TYPE_NAME.equals(typeName)) {
            IntWritable v = getConstValue(numberOI);
            return v.get();
        } else if (BIGINT_TYPE_NAME.equals(typeName)) {
            LongWritable v = getConstValue(numberOI);
            return v.get();
        } else if (SMALLINT_TYPE_NAME.equals(typeName)) {
            ShortWritable v = getConstValue(numberOI);
            return v.get();
        } else if (TINYINT_TYPE_NAME.equals(typeName)) {
            ByteWritable v = getConstValue(numberOI);
            return v.get();
        }
        throw new UDFArgumentException("Unexpected argument type to cast as double: "
                + TypeInfoUtils.getTypeInfoFromObjectInspector(numberOI));
    }

    public static double getAsConstDouble(@Nonnull final ObjectInspector numberOI)
            throws UDFArgumentException {
        final String typeName = numberOI.getTypeName();
        if (DOUBLE_TYPE_NAME.equals(typeName)) {
            DoubleWritable v = getConstValue(numberOI);
            return v.get();
        } else if (FLOAT_TYPE_NAME.equals(typeName)) {
            FloatWritable v = getConstValue(numberOI);
            return v.get();
        } else if (INT_TYPE_NAME.equals(typeName)) {
            IntWritable v = getConstValue(numberOI);
            return v.get();
        } else if (BIGINT_TYPE_NAME.equals(typeName)) {
            LongWritable v = getConstValue(numberOI);
            return v.get();
        } else if (SMALLINT_TYPE_NAME.equals(typeName)) {
            ShortWritable v = getConstValue(numberOI);
            return v.get();
        } else if (TINYINT_TYPE_NAME.equals(typeName)) {
            ByteWritable v = getConstValue(numberOI);
            return v.get();
        }
        throw new UDFArgumentException("Unexpected argument type to cast as double: "
                + TypeInfoUtils.getTypeInfoFromObjectInspector(numberOI));
    }

    @Nonnull
    public static long[] asLongArray(@Nullable final Object argObj,
            @Nonnull final ListObjectInspector listOI, @Nonnull PrimitiveObjectInspector elemOI) {
        if (argObj == null) {
            return null;
        }
        final int length = listOI.getListLength(argObj);
        final long[] ary = new long[length];
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(argObj, i);
            if (o == null) {
                continue;
            }
            ary[i] = PrimitiveObjectInspectorUtils.getLong(o, elemOI);
        }
        return ary;
    }

    @Nonnull
    public static long[] asLongArray(@Nullable final Object argObj,
            @Nonnull final ListObjectInspector listOI, @Nonnull LongObjectInspector elemOI) {
        if (argObj == null) {
            return null;
        }
        final int length = listOI.getListLength(argObj);
        final long[] ary = new long[length];
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(argObj, i);
            if (o == null) {
                continue;
            }
            ary[i] = elemOI.get(o);
        }
        return ary;
    }

    @Nullable
    public static double[] asDoubleArray(@Nullable final Object argObj,
            @Nonnull final ListObjectInspector listOI,
            @Nonnull final PrimitiveObjectInspector elemOI) throws UDFArgumentException {
        return asDoubleArray(argObj, listOI, elemOI, true);
    }

    @Nullable
    public static double[] asDoubleArray(@Nullable final Object argObj,
            @Nonnull final ListObjectInspector listOI,
            @Nonnull final PrimitiveObjectInspector elemOI, final boolean avoidNull)
            throws UDFArgumentException {
        if (argObj == null) {
            return null;
        }
        final int length = listOI.getListLength(argObj);
        final double[] ary = new double[length];
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(argObj, i);
            if (o == null) {
                if (avoidNull) {
                    continue;
                }
                throw new UDFArgumentException("Found null at index " + i);
            }
            double d = PrimitiveObjectInspectorUtils.getDouble(o, elemOI);
            ary[i] = d;
        }
        return ary;
    }

    @Nonnull
    public static void toDoubleArray(@Nullable final Object argObj,
            @Nonnull final ListObjectInspector listOI,
            @Nonnull final PrimitiveObjectInspector elemOI, @Nonnull final double[] out,
            final boolean avoidNull) throws UDFArgumentException {
        if (argObj == null) {
            return;
        }
        final int length = listOI.getListLength(argObj);
        if (out.length != length) {
            throw new UDFArgumentException("Dimension mismatched. Expected: " + out.length
                    + ", Actual: " + length);
        }
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(argObj, i);
            if (o == null) {
                if (avoidNull) {
                    continue;
                }
                throw new UDFArgumentException("Found null at index " + i);
            }
            double d = PrimitiveObjectInspectorUtils.getDouble(o, elemOI);
            out[i] = d;
        }
        return;
    }

    @Nonnull
    public static void toDoubleArray(@Nullable final Object argObj,
            @Nonnull final ListObjectInspector listOI,
            @Nonnull final PrimitiveObjectInspector elemOI, @Nonnull final double[] out,
            final double nullValue) throws UDFArgumentException {
        if (argObj == null) {
            return;
        }
        final int length = listOI.getListLength(argObj);
        if (out.length != length) {
            throw new UDFArgumentException("Dimension mismatched. Expected: " + out.length
                    + ", Actual: " + length);
        }
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(argObj, i);
            if (o == null) {
                out[i] = nullValue;
                continue;
            }
            double d = PrimitiveObjectInspectorUtils.getDouble(o, elemOI);
            out[i] = d;
        }
        return;
    }

    /**
     * @return the number of true bits
     */
    @Nonnull
    public static int setBits(@Nullable final Object argObj,
            @Nonnull final ListObjectInspector listOI,
            @Nonnull final PrimitiveObjectInspector elemOI, @Nonnull final BitSet bitset)
            throws UDFArgumentException {
        if (argObj == null) {
            return 0;
        }
        int count = 0;
        final int length = listOI.getListLength(argObj);
        for (int i = 0; i < length; i++) {
            Object o = listOI.getListElement(argObj, i);
            if (o == null) {
                continue;
            }
            int index = PrimitiveObjectInspectorUtils.getInt(o, elemOI);
            if (index < 0) {
                throw new UDFArgumentException("Negative index is not allowed: " + index);
            }
            bitset.set(index);
            count++;
        }
        return count;
    }

    @Nonnull
    public static ConstantObjectInspector asConstantObjectInspector(
            @Nonnull final ObjectInspector oi) throws UDFArgumentException {
        if (!ObjectInspectorUtils.isConstantObjectInspector(oi)) {
            throw new UDFArgumentException("argument must be a constant value: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        return (ConstantObjectInspector) oi;
    }

    public static PrimitiveObjectInspector asPrimitiveObjectInspector(
            @Nonnull final ObjectInspector oi) throws UDFArgumentException {
        if (oi.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("Expecting PrimitiveObjectInspector: "
                    + TypeInfoUtils.getTypeInfoFromObjectInspector(oi));
        }
        return (PrimitiveObjectInspector) oi;
    }

    public static StringObjectInspector asStringOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if (!STRING_TYPE_NAME.equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be String: " + argOI.getTypeName());
        }
        return (StringObjectInspector) argOI;
    }

    public static BinaryObjectInspector asBinaryOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if (!BINARY_TYPE_NAME.equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be Binary: " + argOI.getTypeName());
        }
        return (BinaryObjectInspector) argOI;
    }

    public static BooleanObjectInspector asBooleanOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if (!BOOLEAN_TYPE_NAME.equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be Boolean: " + argOI.getTypeName());
        }
        return (BooleanObjectInspector) argOI;
    }

    public static IntObjectInspector asIntOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if (!INT_TYPE_NAME.equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be INT: " + argOI.getTypeName());
        }
        return (IntObjectInspector) argOI;
    }

    public static LongObjectInspector asLongOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if (!BIGINT_TYPE_NAME.equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be BIGINT: " + argOI.getTypeName());
        }
        return (LongObjectInspector) argOI;
    }

    public static DoubleObjectInspector asDoubleOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentException {
        if (!DOUBLE_TYPE_NAME.equals(argOI.getTypeName())) {
            throw new UDFArgumentException("Argument type must be DOUBLE: " + argOI.getTypeName());
        }
        return (DoubleObjectInspector) argOI;
    }

    public static PrimitiveObjectInspector asIntCompatibleOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentTypeException {
        if (argOI.getCategory() != Category.PRIMITIVE) {
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

    public static PrimitiveObjectInspector asLongCompatibleOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentTypeException {
        if (argOI.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
                    + argOI.getTypeName() + " is passed.");
        }
        final PrimitiveObjectInspector oi = (PrimitiveObjectInspector) argOI;
        switch (oi.getPrimitiveCategory()) {
            case LONG:
            case INT:
            case SHORT:
            case BYTE:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case TIMESTAMP:
            case DECIMAL:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Unxpected type '" + argOI.getTypeName()
                        + "' is passed.");
        }
        return oi;
    }

    public static PrimitiveObjectInspector asIntegerOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentTypeException {
        if (argOI.getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but "
                    + argOI.getTypeName() + " is passed.");
        }
        final PrimitiveObjectInspector oi = (PrimitiveObjectInspector) argOI;
        switch (oi.getPrimitiveCategory()) {
            case INT:
            case SHORT:
            case LONG:
            case BYTE:
                break;
            default:
                throw new UDFArgumentTypeException(0, "Unxpected type '" + argOI.getTypeName()
                        + "' is passed.");
        }
        return oi;
    }

    public static PrimitiveObjectInspector asDoubleCompatibleOI(@Nonnull final ObjectInspector argOI)
            throws UDFArgumentTypeException {
        if (argOI.getCategory() != Category.PRIMITIVE) {
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
                throw new UDFArgumentTypeException(0,
                    "Only numeric or string type arguments are accepted but " + argOI.getTypeName()
                            + " is passed.");
        }
        return oi;
    }

    @Nonnull
    public static ListObjectInspector asListOI(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        Category category = oi.getCategory();
        if (category != Category.LIST) {
            throw new UDFArgumentException("Expected List OI but was: " + oi);
        }
        return (ListObjectInspector) oi;
    }

    public static void validateFeatureOI(@Nonnull final ObjectInspector oi)
            throws UDFArgumentException {
        final String typeName = oi.getTypeName();
        if (!STRING_TYPE_NAME.equals(typeName) && !INT_TYPE_NAME.equals(typeName)
                && !BIGINT_TYPE_NAME.equals(typeName)) {
            throw new UDFArgumentException(
                "argument type for a feature must be List of key type [Int|BitInt|Text]: "
                        + typeName);
        }
    }

    @Nonnull
    public static FloatWritable[] newFloatArray(final int size, final float defaultVal) {
        final FloatWritable[] array = new FloatWritable[size];
        for (int i = 0; i < size; i++) {
            array[i] = new FloatWritable(defaultVal);
        }
        return array;
    }

    public static LazySimpleSerDe getKeyValueLineSerde(
            @Nonnull final PrimitiveObjectInspector keyOI,
            @Nonnull final PrimitiveObjectInspector valueOI) throws SerDeException {
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
        if (OIs.length == 0) {
            throw new IllegalArgumentException("OIs must be specified");
        }
        LazySimpleSerDe serde = new LazySimpleSerDe();
        Configuration conf = new Configuration();
        Properties tbl = new Properties();

        StringBuilder columnNames = new StringBuilder();
        StringBuilder columnTypes = new StringBuilder();
        for (int i = 0; i < OIs.length; i++) {
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
