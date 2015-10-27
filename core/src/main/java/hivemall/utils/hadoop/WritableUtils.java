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

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
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
    public static List<LongWritable> newLongList(int size) {
        final LongWritable[] array = new LongWritable[size];
        for(int i = 0, len = array.length; i < len; i++) {
            array[i] = new LongWritable(0L);
        }
        return Arrays.asList(array);
    }

    @Nonnull
    public static List<DoubleWritable> newDoubleList(int size) {
        final DoubleWritable[] array = new DoubleWritable[size];
        for(int i = 0, len = array.length; i < len; i++) {
            array[i] = new DoubleWritable(0.d);
        }
        return Arrays.asList(array);
    }

    @Nonnull
    public static List<LongWritable> toWritableList(@Nonnull final long[] src) {
        final LongWritable[] writables = new LongWritable[src.length];
        for(int i = 0; i < src.length; i++) {
            writables[i] = new LongWritable(src[i]);
        }
        return Arrays.asList(writables);
    }

    @Nonnull
    public static List<DoubleWritable> toWritableList(@Nonnull final double[] src) {
        final DoubleWritable[] writables = new DoubleWritable[src.length];
        for(int i = 0; i < src.length; i++) {
            writables[i] = new DoubleWritable(src[i]);
        }
        return Arrays.asList(writables);
    }

    public static Text val(final String v) {
        return new Text(v);
    }

    public static List<Text> val(final String... v) {
        final Text[] ret = new Text[v.length];
        for(int i = 0; i < v.length; i++) {
            String vi = v[i];
            ret[i] = (vi == null) ? null : new Text(vi);
        }
        return Arrays.asList(ret);
    }

    public static Writable toWritable(Object object) {
        if(object == null) {
            return null; //return NullWritable.get();
        }
        if(object instanceof Writable) {
            return (Writable) object;
        }
        if(object instanceof String) {
            return new Text((String) object);
        }
        if(object instanceof Long) {
            return new VLongWritable((Long) object);
        }
        if(object instanceof Integer) {
            return new VIntWritable((Integer) object);
        }
        if(object instanceof Byte) {
            return new ByteWritable((Byte) object);
        }
        if(object instanceof Double) {
            return new DoubleWritable((Double) object);
        }
        if(object instanceof Float) {
            return new FloatWritable((Float) object);
        }
        if(object instanceof Boolean) {
            return new BooleanWritable((Boolean) object);
        }
        if(object instanceof byte[]) {
            return new BytesWritable((byte[]) object);
        }
        return new BytesWritable(object.toString().getBytes());
    }

}
