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
package hivemall.utils.hadoop;

import hivemall.utils.lang.Preconditions;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

public final class WritableUtils {

    private WritableUtils() {}

    public static IntWritable val(final int v) {
        return new IntWritable(v);
    }

    public static LongWritable val(final long v) {
        return new LongWritable(v);
    }

    public static FloatWritable val(final float v) {
        return new FloatWritable(v);
    }

    public static DoubleWritable val(final double v) {
        return new DoubleWritable(v);
    }

    public static BooleanWritable val(final boolean v) {
        return new BooleanWritable(v);
    }

    @Nonnull
    public static List<LongWritable> newLongList(final int size) {
        // workaround to avoid a bug in Kryo
        // https://issues.apache.org/jira/browse/HIVE-12551
        /*
        final LongWritable[] array = new LongWritable[size];
        for (int i = 0; i < size; i++) {
            array[i] = new LongWritable(0L);
        }
        return Arrays.asList(array);
        */
        final List<LongWritable> list = new ArrayList<LongWritable>(size);
        for (int i = 0; i < size; i++) {
            list.add(new LongWritable(0L));
        }
        return list;
    }

    @Nonnull
    public static List<DoubleWritable> newDoubleList(final int size) {
        return newDoubleList(size, 0.d);
    }

    @Nonnull
    public static List<DoubleWritable> newDoubleList(final int size, final double defaultValue) {
        // workaround to avoid a bug in Kryo
        // https://issues.apache.org/jira/browse/HIVE-12551
        /*
        final DoubleWritable[] array = new DoubleWritable[size];
        for (int i = 0; i < size; i++) {
            array[i] = new DoubleWritable(defaultValue);
        }
        return Arrays.asList(array);
        */
        final List<DoubleWritable> list = new ArrayList<DoubleWritable>(size);
        for (int i = 0; i < size; i++) {
            list.add(new DoubleWritable(defaultValue));
        }
        return list;
    }

    @Nonnull
    public static List<LongWritable> toWritableList(@Nonnull final long[] src) {
        // workaround to avoid a bug in Kryo
        // https://issues.apache.org/jira/browse/HIVE-12551
        /*
        final LongWritable[] writables = new LongWritable[src.length];
        for (int i = 0; i < src.length; i++) {
            writables[i] = new LongWritable(src[i]);
        }
        return Arrays.asList(writables);
        */
        final List<LongWritable> list = new ArrayList<LongWritable>(src.length);
        for (int i = 0; i < src.length; i++) {
            list.add(new LongWritable(src[i]));
        }
        return list;
    }

    @Nonnull
    public static List<DoubleWritable> toWritableList(@Nonnull final double[] src) {
        // workaround to avoid a bug in Kryo
        // https://issues.apache.org/jira/browse/HIVE-12551
        /*
        final DoubleWritable[] writables = new DoubleWritable[src.length];
        for (int i = 0; i < src.length; i++) {
            writables[i] = new DoubleWritable(src[i]);
        }
        return Arrays.asList(writables);
        */
        final List<DoubleWritable> list = new ArrayList<DoubleWritable>(src.length);
        for (int i = 0; i < src.length; i++) {
            list.add(new DoubleWritable(src[i]));
        }
        return list;
    }

    public static Text val(final String v) {
        return new Text(v);
    }

    public static List<Text> val(final String... v) {
        // workaround to avoid a bug in Kryo
        // https://issues.apache.org/jira/browse/HIVE-12551
        /*
        final Text[] ret = new Text[v.length];
        for (int i = 0; i < v.length; i++) {
            String vi = v[i];
            ret[i] = (vi == null) ? null : new Text(vi);
        }
        return Arrays.asList(ret);
        */
        final List<Text> list = new ArrayList<Text>(v.length);
        for (int i = 0; i < v.length; i++) {
            String vi = v[i];
            Text ti = (vi == null) ? null : new Text(vi);
            list.add(ti);
        }
        return list;
    }

    public static Writable toWritable(Object object) {
        if (object == null) {
            return null; //return NullWritable.get();
        }
        if (object instanceof Writable) {
            return (Writable) object;
        }
        if (object instanceof String) {
            return new Text((String) object);
        }
        if (object instanceof Long) {
            return new VLongWritable((Long) object);
        }
        if (object instanceof Integer) {
            return new VIntWritable((Integer) object);
        }
        if (object instanceof Byte) {
            return new ByteWritable((Byte) object);
        }
        if (object instanceof Double) {
            return new DoubleWritable((Double) object);
        }
        if (object instanceof Float) {
            return new FloatWritable((Float) object);
        }
        if (object instanceof Boolean) {
            return new BooleanWritable((Boolean) object);
        }
        if (object instanceof byte[]) {
            return new BytesWritable((byte[]) object);
        }
        return new BytesWritable(object.toString().getBytes());
    }

    @Nonnull
    public static Writable copyToWritable(@Nonnull final Object obj,
            @CheckForNull final PrimitiveObjectInspector oi) {
        Preconditions.checkNotNull(oi);
        Object ret = ObjectInspectorUtils.copyToStandardObject(obj, oi,
            ObjectInspectorCopyOption.WRITABLE);
        return (Writable) ret;
    }

}
