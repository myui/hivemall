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
package hivemall.fm;

import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;

public final class Feature {
    int index;
    double value;

    public Feature(int index, double value) {
        this.index = index;
        this.value = value;
    }

    public Feature(@Nonnull ByteBuffer src) {
        readFrom(src);
    }

    public int bytes() {
        return (Integer.SIZE + Double.SIZE) / 8;
    }

    public void writeTo(@Nonnull final ByteBuffer dst) {
        dst.putInt(index);
        dst.putDouble(value);
    }

    public void readFrom(@Nonnull final ByteBuffer src) {
        this.index = src.getInt();
        this.value = src.getDouble();
    }

    @Nonnull
    public static Feature parse(@Nonnull final String s) throws HiveException {
        int pos = s.indexOf(":");
        String s1 = s.substring(0, pos);
        String s2 = s.substring(pos + 1);
        int index = Integer.parseInt(s1);
        if(index < 0) {
            throw new HiveException("Feature index MUST be greater than 0: " + s);
        }
        double value = Double.parseDouble(s2);
        return new Feature(index, value);
    }

    public static void parse(@Nonnull final String s, @Nonnull final Feature probe)
            throws HiveException {
        int pos = s.indexOf(":");
        String s1 = s.substring(0, pos);
        String s2 = s.substring(pos + 1);
        int index = Integer.parseInt(s1);
        if(index < 0) {
            throw new HiveException("Feature index MUST be greater than 0: " + s);
        }
        double value = Double.parseDouble(s2);
        probe.index = index;
        probe.value = value;
    }

    @Nullable
    public static Feature[] parseFeatures(@Nonnull final Object arg, @Nonnull final ListObjectInspector listOI)
            throws HiveException {
        if(arg == null) {
            return null;
        }
        final int length = listOI.getListLength(arg);
        final Feature[] ary = new Feature[length];
        int j = 0;
        for(int i = 0; i < length; i++) {
            Object o = listOI.getListElement(arg, i);
            if(o == null) {
                continue;
            }
            String s = o.toString();
            Feature f = parse(s);
            ary[j] = f;
            j++;
        }
        if(j == length) {
            return ary;
        } else {
            Feature[] dst = new Feature[j];
            System.arraycopy(ary, 0, dst, 0, j);
            return dst;
        }
    }

    @Nullable
    public static Feature[] parseFeatures(@Nonnull final Object arg, @Nonnull final ListObjectInspector listOI, @Nullable final Feature[] probes)
            throws HiveException {
        if(arg == null) {
            return null;
        }

        final int length = listOI.getListLength(arg);
        final Feature[] ary;
        if(probes != null && probes.length == length) {
            ary = probes;
        } else {
            ary = new Feature[length];
        }

        int j = 0;
        for(int i = 0; i < length; i++) {
            Object o = listOI.getListElement(arg, i);
            if(o == null) {
                continue;
            }
            String s = o.toString();
            Feature f = parse(s);
            ary[j] = f;
            j++;
        }
        if(j == length) {
            return ary;
        } else {
            Feature[] dst = new Feature[j];
            System.arraycopy(ary, 0, dst, 0, j);
            return dst;
        }
    }

    @Nonnull
    public static Feature[] parseFeatures(@Nonnull final List<String> features)
            throws HiveException {
        final int length = features.size();
        final Feature[] ary = new Feature[length];
        int j = 0;
        for(int i = 0; i < length; i++) {
            String s = features.get(i);
            if(s == null) {
                continue;
            }
            Feature f = parse(s);
            ary[j] = f;
            j++;
        }
        if(j == length) {
            return ary;
        } else {
            Feature[] dst = new Feature[j];
            System.arraycopy(ary, 0, dst, 0, j);
            return dst;
        }
    }

    @Nonnull
    public static Feature[] parseFeatures(@Nonnull final List<String> features, @Nullable final Feature[] probes)
            throws HiveException {
        final int length = features.size();
        final Feature[] ary;
        if(probes != null && probes.length == length) {
            ary = probes;
        } else {
            ary = new Feature[length];
        }

        int j = 0;
        for(int i = 0; i < length; i++) {
            String s = features.get(i);
            if(s == null) {
                continue;
            }
            Feature f = parse(s);
            ary[j] = f;
            j++;
        }
        if(j == length) {
            return ary;
        } else {
            Feature[] dst = new Feature[j];
            System.arraycopy(ary, 0, dst, 0, j);
            return dst;
        }
    }

    public static int requiredBytes(@Nonnull final Feature[] x) {
        int ret = 0;
        for(Feature f : x) {
            assert (f != null);
            ret += f.bytes();
        }
        return ret;
    }

    @Override
    public String toString() {
        return "Feature [index=" + index + ", value=" + value + "]";
    }

}
